/*
 * SPDX-FileCopyrightText: 2025
 * SPDX-License-Identifier: CC0-1.0
 *
 * Datouboss DT4862-BF Inverter Monitor — ESP-IDF v5.x
 * Comunicación RS232 (protocolo PI30) + WiFi STA + WebSocket + SPIFFS histórico
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "driver/uart.h"
#include "driver/gpio.h"
#include "esp_http_server.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "mdns.h"
#include "esp_heap_caps.h"
#include "cJSON.h"
#include "esp_system.h"
#include "lwip/sockets.h"
#include "esp_spiffs.h"
#include "esp_sntp.h"
#include <time.h>
#include <sys/time.h>

static const char *TAG      = "INV_PI30";
static const char *TAG_WIFI = "WIFI";

/* ---- WiFi STA ---- */
#define WIFI_RETRY_MAX   10

/* ---- AP provisioning ---- */
#define AP_SSID          "INV-Setup"
#define AP_CHANNEL       1
#define AP_MAX_CONN      4

/* ---- NVS keys para credenciales WiFi ---- */
#define NVS_WIFI_NS      "wifi_cfg"
#define NVS_WIFI_SSID    "ssid"
#define NVS_WIFI_PASS    "pass"

/* bits del EventGroup WiFi */
#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT      BIT1

static EventGroupHandle_t s_wifi_event_group = nullptr;
static int                s_wifi_retry_count  = 0;

/* ---- UART PI30 (configurado por Kconfig / menuconfig) ---- */
#define INV_UART_PORT    ((uart_port_t)CONFIG_INV_UART_PORT_NUM)
#define INV_UART_TX      CONFIG_INV_UART_TX_GPIO
#define INV_UART_RX      CONFIG_INV_UART_RX_GPIO
#define INV_UART_BAUD    CONFIG_INV_UART_BAUD
#define INV_UART_RXBUF   512
#define INV_CMD_TIMEOUT_MS  2500   /* timeout espera respuesta inversor */

/* ---- Push remoto ---- */
#define REMOTE_PUSH_URL   "https://luaresearch.es/inv/update.php"
#define REMOTE_API_KEY    "b7e4a1f3c9d2e8b6a0f5c3d7e2b9a4f1c6d0e3b8a5f2c7d1e6b3a0f8c4d9e2b5"
#define REMOTE_PUSH_MS    5000

/* ---- Push histórico remoto ---- */
#define REMOTE_HIST_URL   "https://luaresearch.es/inv/update_history.php"
#define REMOTE_HIST_USER  "lua"
#define REMOTE_HIST_PASS  "0304"

/* ---- Intervalo de agregación: 5 minutos ---- */
#define HIST_INTERVAL_S   300

/* ---- Epoch mínima para SNTP sincronizado (2020-01-01) ---- */
#define SNTP_EPOCH_MIN    1577836800UL

/* ---- Intervalos de polling PI30 ---- */
#define POLL_QPIGS_MS     5000
#define POLL_QMOD_MS      30000
#define POLL_QPIWS_MS     60000

/* ------------------------------------------------------------------ */
/* ---- Estado del inversor (mutex protegido entre tareas)        ---- */
/* ------------------------------------------------------------------ */
struct InvState {
    /* QPIGS — 21 campos de estado en tiempo real */
    float    ac_in_v;          /* V  — tensión red entrada           */
    float    ac_in_hz;         /* Hz — frecuencia red entrada        */
    float    ac_out_v;         /* V  — tensión salida AC             */
    float    ac_out_hz;        /* Hz — frecuencia salida AC          */
    int      ac_out_va;        /* VA — potencia aparente salida      */
    int      ac_out_w;         /* W  — potencia activa salida        */
    int      load_pct;         /* %  — porcentaje de carga           */
    int      bus_v;            /* V  — tensión bus DC interno        */
    float    bat_v;            /* V  — tensión batería               */
    int      bat_charge_a;     /* A  — corriente carga batería       */
    int      bat_cap_pct;      /* %  — capacidad batería             */
    int      heatsink_c;       /* °C — temperatura disipador         */
    float    pv_a;             /* A  — corriente PV hacia batería    */
    float    pv_v;             /* V  — tensión entrada PV            */
    float    bat_scc_v;        /* V  — tensión bat medida por SCC    */
    int      bat_discharge_a;  /* A  — corriente descarga batería    */
    uint8_t  dev_flags;        /* 8 bits estado dispositivo          */
    int      pv_w;             /* W  — potencia PV entrada           */
    uint8_t  dev_flags2;       /* 3 bits estado 2                    */
    /* QMOD — modo operativo (un carácter) */
    char     mode;             /* L=Line B=Battery F=Fault S=Standby P=PowerOn H=Save Y=Bypass */
    /* QPIWS — bits de avisos y fallos */
    uint32_t warnings;         /* 32 bits: posición 0=MSB de payload[0..31] */
    /* Meta */
    char     wifi_ip[16];
    bool     uart_ok;          /* última comunicación UART correcta  */
    uint16_t valid;            /* bit0=QPIGS, bit1=QMOD, bit2=QPIWS  */
};

static InvState          s_inv       = {};
static SemaphoreHandle_t s_inv_mutex = nullptr;

/* ---- Registro histórico binario (12 bytes por entrada) ---- */
typedef struct {
    uint32_t ts;       /* Unix timestamp inicio del intervalo */
    float    bat_v;    /* promedio tensión batería (V)        */
    float    pv_w;     /* promedio potencia PV (W)            */
} HistRecord;

/* ---- Acumuladores 1 Hz → promedio de HIST_INTERVAL_S ---- */
static float             s_batv_sum   = 0.0f;
static float             s_pvw_sum    = 0.0f;
static int               s_sample_cnt = 0;
static SemaphoreHandle_t s_hist_mutex = nullptr;

/* ---- HTTP / WebSocket globals ---- */
#define WS_MAX_CLIENTS 4
static httpd_handle_t    s_httpd     = nullptr;
static int               s_ws_fds[WS_MAX_CLIENTS];

/* ------------------------------------------------------------------ */
/* ---- Utilidades                                                ---- */
/* ------------------------------------------------------------------ */
static inline uint64_t millis(void)
{
    return (uint64_t)(esp_timer_get_time() / 1000ULL);
}

static inline void delay(uint32_t ms)
{
    vTaskDelay(pdMS_TO_TICKS(ms));
}

/* ================================================================== */
/* ---- Protocolo PI30: CRC y comunicación UART                  ---- */
/* ================================================================== */

/*
 * CRC-16 PI30 — tabla de 16 entradas (idéntico a crcPI de mpp-solar).
 * Calcula el checksum de `len` bytes a partir de `data`.
 * Ajusta los bytes de salida para evitar valores reservados (0x00, 0x0A, 0x0D, 0x28).
 */
static void crc_pi(const uint8_t *data, size_t len, uint8_t *crc_h, uint8_t *crc_l)
{
    static const uint16_t crc_ta[16] = {
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
        0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    };
    uint16_t crc = 0;
    for (size_t i = 0; i < len; i++) {
        uint8_t c  = data[i];
        uint8_t da = ((crc >> 8) & 0xFF) >> 4;
        crc = (crc << 4) & 0xFFFF;
        crc ^= crc_ta[da ^ (c >> 4)];
        da  = ((crc >> 8) & 0xFF) >> 4;
        crc = (crc << 4) & 0xFFFF;
        crc ^= crc_ta[da ^ (c & 0x0F)];
    }
    uint8_t lo = crc & 0xFF;
    uint8_t hi = (crc >> 8) & 0xFF;
    if (lo == 0x28 || lo == 0x0D || lo == 0x0A || lo == 0x00) lo++;
    if (hi == 0x28 || hi == 0x0D || hi == 0x0A || hi == 0x00) hi++;
    *crc_h = hi;
    *crc_l = lo;
}

/*
 * Verifica el CRC de una respuesta PI30 completa.
 * buf: frame completo (empieza con '(', termina con CRC_H CRC_L '\r').
 * len: longitud total del frame.
 */
static bool crc_pi_verify(const uint8_t *buf, size_t len)
{
    if (len < 4) return false;
    uint8_t expected_h, expected_l;
    crc_pi(buf, len - 3, &expected_h, &expected_l);
    return (buf[len - 3] == expected_h && buf[len - 2] == expected_l);
}

/* ---- Inicializar UART para comunicación RS232 con el inversor ---- */
static void uart_inv_init(void)
{
    uart_config_t ucfg = {};
    ucfg.baud_rate  = INV_UART_BAUD;
    ucfg.data_bits  = UART_DATA_8_BITS;
    ucfg.parity     = UART_PARITY_DISABLE;
    ucfg.stop_bits  = UART_STOP_BITS_1;
    ucfg.flow_ctrl  = UART_HW_FLOWCTRL_DISABLE;
    ucfg.source_clk = UART_SCLK_DEFAULT;

    ESP_ERROR_CHECK(uart_driver_install(INV_UART_PORT, INV_UART_RXBUF, 0, 0, NULL, 0));
    ESP_ERROR_CHECK(uart_param_config(INV_UART_PORT, &ucfg));
    ESP_ERROR_CHECK(uart_set_pin(INV_UART_PORT, INV_UART_TX, INV_UART_RX,
                                 UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE));
    ESP_LOGI(TAG, "UART%d init: %d baud, TX=GPIO%d, RX=GPIO%d",
             (int)INV_UART_PORT, INV_UART_BAUD, INV_UART_TX, INV_UART_RX);
}

/*
 * Envía un comando PI30 y lee la respuesta.
 *
 * cmd:         string null-terminated del comando (p.ej. "QPIGS").
 * payload_buf: buffer donde se copia el payload decodificado
 *              (sin '(' inicial, sin CRC_H CRC_L '\r' final).
 * buf_len:     tamaño de payload_buf.
 *
 * Retorna el número de bytes escritos en payload_buf (>0 = OK), 0 = error.
 */
static int send_pi30_cmd(const char *cmd, char *payload_buf, size_t buf_len)
{
    size_t cmd_len = strlen(cmd);
    if (cmd_len + 3 > 32) return 0;    /* nunca debería ocurrir */

    /* Construir trama: <CMD_ASCII><CRC_H><CRC_L><CR> */
    uint8_t frame[32];
    memcpy(frame, cmd, cmd_len);
    crc_pi((const uint8_t *)cmd, cmd_len, &frame[cmd_len], &frame[cmd_len + 1]);
    frame[cmd_len + 2] = '\r';

    uart_flush_input(INV_UART_PORT);

    int written = uart_write_bytes(INV_UART_PORT, frame, cmd_len + 3);
    if (written < (int)(cmd_len + 3)) {
        ESP_LOGW(TAG, "UART write incompleto para cmd '%s'", cmd);
        return 0;
    }

    /* Leer respuesta byte a byte hasta '\r' o timeout */
    uint8_t  rx[288];    /* suficiente para QPIGS (~110 bytes) y QPIWS (~40 bytes) */
    size_t   rx_len  = 0;
    uint64_t deadline = millis() + INV_CMD_TIMEOUT_MS;

    while (millis() < deadline && rx_len < sizeof(rx) - 1) {
        uint8_t b;
        int n = uart_read_bytes(INV_UART_PORT, &b, 1, pdMS_TO_TICKS(20));
        if (n <= 0) continue;
        rx[rx_len++] = b;
        if (b == '\r') break;
    }

    if (rx_len < 4 || rx[0] != '(') {
        ESP_LOGW(TAG, "PI30 respuesta inválida para '%s' (len=%d, first=0x%02X)",
                 cmd, (int)rx_len, rx_len > 0 ? rx[0] : 0);
        return 0;
    }

    if (!crc_pi_verify(rx, rx_len)) {
        ESP_LOGW(TAG, "PI30 CRC incorrecto para '%s'", cmd);
        return 0;
    }

    /* Copiar payload: rx[1..rx_len-4]  (quitar '(' y CRC_H CRC_L '\r') */
    size_t payload_len = rx_len - 4;
    if (payload_len >= buf_len) payload_len = buf_len - 1;
    memcpy(payload_buf, rx + 1, payload_len);
    payload_buf[payload_len] = '\0';
    return (int)payload_len;
}

/* ================================================================== */
/* ---- Parsers de respuestas PI30                                ---- */
/* ================================================================== */

/*
 * QPIGS — General Status Parameters (21 campos separados por espacio).
 * Ejemplo real: "000.0 00.0 230.0 49.9 0161 0119 003 460 57.50 012 100 0069
 *               0014 103.8 57.45 00000 00110110 00 00 00856 010"
 */
static bool parse_qpigs(const char *payload)
{
    char buf[220];
    strncpy(buf, payload, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    char *fields[24];
    int   nf  = 0;
    char *tok = strtok(buf, " ");
    while (tok && nf < 24) { fields[nf++] = tok; tok = strtok(nullptr, " "); }

    if (nf < 17) {
        ESP_LOGW(TAG, "QPIGS: solo %d campos (mínimo 17)", nf);
        return false;
    }

    xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
    s_inv.ac_in_v         = strtof(fields[0],  nullptr);
    s_inv.ac_in_hz        = strtof(fields[1],  nullptr);
    s_inv.ac_out_v        = strtof(fields[2],  nullptr);
    s_inv.ac_out_hz       = strtof(fields[3],  nullptr);
    s_inv.ac_out_va       = (int)strtol(fields[4],  nullptr, 10);
    s_inv.ac_out_w        = (int)strtol(fields[5],  nullptr, 10);
    s_inv.load_pct        = (int)strtol(fields[6],  nullptr, 10);
    s_inv.bus_v           = (int)strtol(fields[7],  nullptr, 10);
    s_inv.bat_v           = strtof(fields[8],  nullptr);
    s_inv.bat_charge_a    = (int)strtol(fields[9],  nullptr, 10);
    s_inv.bat_cap_pct     = (int)strtol(fields[10], nullptr, 10);
    s_inv.heatsink_c      = (int)strtol(fields[11], nullptr, 10);
    s_inv.pv_a            = strtof(fields[12], nullptr);
    s_inv.pv_v            = strtof(fields[13], nullptr);
    s_inv.bat_scc_v       = strtof(fields[14], nullptr);
    s_inv.bat_discharge_a = (int)strtol(fields[15], nullptr, 10);

    /* Campo 16: string de 8 chars '0'/'1' — flags dispositivo
     * Bit7=SBU, Bit6=CfgChanged, Bit5=SCCFwUpdated, Bit4=LoadOn,
     * Bit3=BatVSteady, Bit2=ChargingOn, Bit1=SCCChargingOn, Bit0=ACChargingOn */
    if (strlen(fields[16]) >= 8) {
        uint8_t f = 0;
        for (int i = 0; i < 8; i++)
            if (fields[16][i] == '1') f |= (uint8_t)(1u << (7 - i));
        s_inv.dev_flags = f;
    }

    /* Campo 19: PV Input Power (W) — presente en PI30 4000/5000 series */
    if (nf > 19) s_inv.pv_w = (int)strtol(fields[19], nullptr, 10);

    /* Campo 20: string de 3 chars — flags2: ChargingToFloat, SwitchedOn, Reserved */
    if (nf > 20 && strlen(fields[20]) >= 3) {
        uint8_t f2 = 0;
        for (int i = 0; i < 3; i++)
            if (fields[20][i] == '1') f2 |= (uint8_t)(1u << (2 - i));
        s_inv.dev_flags2 = f2;
    }

    s_inv.uart_ok  = true;
    s_inv.valid   |= (1u << 0);
    xSemaphoreGive(s_inv_mutex);

    ESP_LOGD(TAG, "QPIGS ok: Vbat=%.2f Ppv=%dW carga=%d%%",
             s_inv.bat_v, s_inv.pv_w, s_inv.bat_cap_pct);
    return true;
}

/*
 * QMOD — Device Mode (un solo carácter).
 * L=Line, B=Battery, F=Fault, S=Standby, P=PowerOn, H=PowerSaving, Y=Bypass
 */
static bool parse_qmod(const char *payload)
{
    if (payload[0] == '\0') return false;
    xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
    s_inv.mode     = payload[0];
    s_inv.uart_ok  = true;
    s_inv.valid   |= (1u << 1);
    xSemaphoreGive(s_inv_mutex);
    ESP_LOGD(TAG, "QMOD ok: mode='%c'", payload[0]);
    return true;
}

/*
 * QPIWS — Warning Status (32 chars ASCII '0'/'1').
 * Los 32 caracteres mapean a bits 31..0 del campo warnings.
 * payload[0]→bit31, payload[1]→bit30, ..., payload[31]→bit0
 */
static bool parse_qpiws(const char *payload)
{
    if (strlen(payload) < 32) return false;
    uint32_t w = 0;
    for (int i = 0; i < 32; i++)
        if (payload[i] == '1') w |= (1u << (31 - i));
    xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
    s_inv.warnings = w;
    s_inv.uart_ok  = true;
    s_inv.valid   |= (1u << 2);
    xSemaphoreGive(s_inv_mutex);
    ESP_LOGD(TAG, "QPIWS ok: 0x%08lX", (unsigned long)w);
    return true;
}

/* ================================================================== */
/* ---- JSON de estado para WebSocket y push remoto              ---- */
/* ================================================================== */

extern const uint8_t inv_ui_html_start[] asm("_binary_inv_ui_html_start");
extern const uint8_t inv_ui_html_end[]   asm("_binary_inv_ui_html_end");

static const char *mode_name(char m)
{
    switch (m) {
        case 'P': return "Power On";
        case 'S': return "Standby";
        case 'L': return "Line";
        case 'B': return "Battery";
        case 'F': return "Fault";
        case 'H': return "Power Saving";
        case 'Y': return "Bypass";
        default:  return "Unknown";
    }
}

static char *build_status_json(void)
{
    if (!s_inv_mutex) return nullptr;
    cJSON *root = cJSON_CreateObject();
    if (!root) return nullptr;
    cJSON_AddStringToObject(root, "t", "status");

    xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
    InvState snap = s_inv;   /* copia local bajo mutex */
    xSemaphoreGive(s_inv_mutex);

    cJSON *conn = cJSON_AddObjectToObject(root, "conn");
    cJSON_AddBoolToObject(conn,   "uart_ok", snap.uart_ok);
    cJSON_AddStringToObject(conn, "ip",      snap.wifi_ip[0] ? snap.wifi_ip : "--");

    if (snap.valid & (1u << 0)) {
        cJSON *pv = cJSON_AddObjectToObject(root, "pv");
        cJSON_AddNumberToObject(pv, "v", (double)snap.pv_v);
        cJSON_AddNumberToObject(pv, "a", (double)snap.pv_a);
        cJSON_AddNumberToObject(pv, "w", snap.pv_w);

        cJSON *bat = cJSON_AddObjectToObject(root, "bat");
        cJSON_AddNumberToObject(bat, "v",        (double)snap.bat_v);
        cJSON_AddNumberToObject(bat, "cap_pct",  snap.bat_cap_pct);
        cJSON_AddNumberToObject(bat, "charge_a", snap.bat_charge_a);
        cJSON_AddNumberToObject(bat, "disch_a",  snap.bat_discharge_a);
        cJSON_AddNumberToObject(bat, "scc_v",    (double)snap.bat_scc_v);
        cJSON_AddNumberToObject(bat, "bus_v",    snap.bus_v);

        cJSON *ac_in = cJSON_AddObjectToObject(root, "ac_in");
        cJSON_AddNumberToObject(ac_in, "v",  (double)snap.ac_in_v);
        cJSON_AddNumberToObject(ac_in, "hz", (double)snap.ac_in_hz);

        cJSON *ac_out = cJSON_AddObjectToObject(root, "ac_out");
        cJSON_AddNumberToObject(ac_out, "v",       (double)snap.ac_out_v);
        cJSON_AddNumberToObject(ac_out, "hz",      (double)snap.ac_out_hz);
        cJSON_AddNumberToObject(ac_out, "va",      snap.ac_out_va);
        cJSON_AddNumberToObject(ac_out, "w",       snap.ac_out_w);
        cJSON_AddNumberToObject(ac_out, "load_pct", snap.load_pct);

        cJSON *temp = cJSON_AddObjectToObject(root, "temp");
        cJSON_AddNumberToObject(temp, "heatsink_c", snap.heatsink_c);

        /* Descodificar dev_flags (8 bits):
         * Bit4=LoadOn, Bit2=ChargingOn, Bit1=SCCChargingOn, Bit0=ACChargingOn */
        cJSON *flags = cJSON_AddObjectToObject(root, "flags");
        cJSON_AddBoolToObject(flags, "load_on",      (snap.dev_flags >> 4) & 1);
        cJSON_AddBoolToObject(flags, "charging",     (snap.dev_flags >> 2) & 1);
        cJSON_AddBoolToObject(flags, "scc_charging", (snap.dev_flags >> 1) & 1);
        cJSON_AddBoolToObject(flags, "ac_charging",  (snap.dev_flags >> 0) & 1);
        /* dev_flags2 Bit2=ChargingToFloat */
        cJSON_AddBoolToObject(flags, "float_charge", (snap.dev_flags2 >> 2) & 1);
    }

    if (snap.valid & (1u << 1)) {
        char mstr[2] = { snap.mode, '\0' };
        cJSON *mod = cJSON_AddObjectToObject(root, "mode");
        cJSON_AddStringToObject(mod, "code", mstr);
        cJSON_AddStringToObject(mod, "name", mode_name(snap.mode));
    }

    if (snap.valid & (1u << 2)) {
        /* Mapeo bit → posición en payload QPIWS (payload[i] → bit 31-i):
         * bit30=InverterFault, bit29=BusOver, bit28=BusUnder, bit27=BusSoftFail,
         * bit26=LineFail, bit25=OPVShort, bit24=InvVLow, bit23=InvVHigh,
         * bit22=OverTemp, bit21=FanLocked, bit20=BatOverVolt, bit19=BatLowAlarm,
         * bit18=Reserved, bit17=BatUnderShutdown, bit16=Reserved,
         * bit15=Overload, bit14=EEPROMFault, bit13=InvOverCurrent,
         * bit12=InvSoftFail, bit11=SelfTestFail, bit10=OPDCOverVolt,
         * bit9=BatOpen, bit8=CurrentSensorFail, bit7=BatShort,
         * bit6=PowerLimit, bit5=PVVoltHigh, bit4=MPPTOverloadFault,
         * bit3=MPPTOverloadWarn, bit2=BatTooLowCharge */
        uint32_t w = snap.warnings;
        cJSON *warn = cJSON_AddObjectToObject(root, "warnings");
        cJSON_AddNumberToObject(warn, "bits",                (double)w);
        cJSON_AddBoolToObject(warn,   "inverter_fault",      (w >> 30) & 1);
        cJSON_AddBoolToObject(warn,   "bus_over",            (w >> 29) & 1);
        cJSON_AddBoolToObject(warn,   "bus_under",           (w >> 28) & 1);
        cJSON_AddBoolToObject(warn,   "line_fail",           (w >> 26) & 1);
        cJSON_AddBoolToObject(warn,   "inv_v_low",           (w >> 24) & 1);
        cJSON_AddBoolToObject(warn,   "inv_v_high",          (w >> 23) & 1);
        cJSON_AddBoolToObject(warn,   "overtemp",            (w >> 22) & 1);
        cJSON_AddBoolToObject(warn,   "fan_locked",          (w >> 21) & 1);
        cJSON_AddBoolToObject(warn,   "bat_overvolt",        (w >> 20) & 1);
        cJSON_AddBoolToObject(warn,   "bat_low_alarm",       (w >> 19) & 1);
        cJSON_AddBoolToObject(warn,   "bat_under_shutdown",  (w >> 17) & 1);
        cJSON_AddBoolToObject(warn,   "overload",            (w >> 15) & 1);
        cJSON_AddBoolToObject(warn,   "eeprom_fault",        (w >> 14) & 1);
        cJSON_AddBoolToObject(warn,   "inv_overcurrent",     (w >> 13) & 1);
        cJSON_AddBoolToObject(warn,   "bat_short",           (w >>  7) & 1);
        cJSON_AddBoolToObject(warn,   "power_limit",         (w >>  6) & 1);
        cJSON_AddBoolToObject(warn,   "pv_overvolt",         (w >>  5) & 1);
        cJSON_AddBoolToObject(warn,   "mppt_overload_fault", (w >>  4) & 1);
        cJSON_AddBoolToObject(warn,   "mppt_overload_warn",  (w >>  3) & 1);
        cJSON_AddBoolToObject(warn,   "bat_low_charge",      (w >>  2) & 1);
    }

    cJSON_AddNumberToObject(root, "uptime",
                            (double)(esp_timer_get_time() / 1000000ULL));
    cJSON_AddNumberToObject(root, "heap",
                            (double)heap_caps_get_free_size(MALLOC_CAP_DEFAULT));

    char *str = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return str;
}

/* ================================================================== */
/* ---- SPIFFS, SNTP e histórico                                  ---- */
/* ================================================================== */

static void spiffs_init(void)
{
    esp_vfs_spiffs_conf_t conf = {
        .base_path              = "/spiffs",
        .partition_label        = nullptr,
        .max_files              = 4,
        .format_if_mount_failed = true,
    };
    esp_err_t ret = esp_vfs_spiffs_register(&conf);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "SPIFFS mount falló: %s", esp_err_to_name(ret));
    } else {
        size_t total = 0, used = 0;
        esp_spiffs_info(nullptr, &total, &used);
        ESP_LOGI(TAG, "SPIFFS: %u KB total, %u KB usado",
                 (unsigned)(total / 1024), (unsigned)(used / 1024));
    }
}

static void sntp_init_fn(void)
{
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_setservername(1, "time.google.com");
    esp_sntp_init();
    ESP_LOGI(TAG, "SNTP iniciado");
}

static void hist_file_path(int year, char *buf, size_t buf_len)
{
    snprintf(buf, buf_len, "/spiffs/hist_%d.bin", year);
}

static void cleanup_old_hist_files(int current_year)
{
    char path[32];
    for (int y = current_year - 6; y < current_year - 1; y++) {
        hist_file_path(y, path, sizeof(path));
        remove(path);
    }
}

static void append_hist_record(const HistRecord *rec)
{
    time_t t = (time_t)rec->ts;
    struct tm tm_info;
    localtime_r(&t, &tm_info);
    int  year = tm_info.tm_year + 1900;
    char path[32];
    hist_file_path(year, path, sizeof(path));
    FILE *f = fopen(path, "ab");
    if (!f) { ESP_LOGW(TAG, "Hist: no se pudo abrir %s", path); return; }
    fwrite(rec, sizeof(HistRecord), 1, f);
    fclose(f);
    ESP_LOGD(TAG, "Hist: ts=%lu vbat=%.2f pvw=%.0f",
             (unsigned long)rec->ts, rec->bat_v, rec->pv_w);
}

static void remote_push_hist(const HistRecord *rec)
{
    {
        xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
        bool has_ip = (s_inv.wifi_ip[0] != '\0');
        xSemaphoreGive(s_inv_mutex);
        if (!has_ip) return;
    }

    char body[96];
    snprintf(body, sizeof(body), "{\"ts\":%lu,\"v\":%.3f,\"p\":%.1f}",
             (unsigned long)rec->ts, rec->bat_v, rec->pv_w);

    esp_http_client_config_t cfg = {};
    cfg.url               = REMOTE_HIST_URL;
    cfg.method            = HTTP_METHOD_POST;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.timeout_ms        = 8000;
    cfg.username          = REMOTE_HIST_USER;
    cfg.password          = REMOTE_HIST_PASS;
    cfg.auth_type         = HTTP_AUTH_TYPE_BASIC;

    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    esp_http_client_set_header(client, "Content-Type", "application/json");
    esp_http_client_set_header(client, "X-API-Key",    REMOTE_API_KEY);
    esp_http_client_set_post_field(client, body, (int)strlen(body));

    esp_err_t err = esp_http_client_perform(client);
    if (err == ESP_OK)
        ESP_LOGI(TAG, "Push hist: HTTP %d", esp_http_client_get_status_code(client));
    else
        ESP_LOGW(TAG, "Push hist falló: %s", esp_err_to_name(err));
    esp_http_client_cleanup(client);
}

/* ---- Tarea: agrega muestras y escribe en SPIFFS cada HIST_INTERVAL_S ---- */
static void hist_task(void *pvParameters)
{
    for (;;) {
        if (time(nullptr) > (time_t)SNTP_EPOCH_MIN) break;
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
    ESP_LOGI(TAG, "SNTP sincronizado — histórico activado");

    for (;;) {
        vTaskDelay(pdMS_TO_TICKS((uint32_t)HIST_INTERVAL_S * 1000));

        time_t now = time(nullptr);
        if (now <= (time_t)SNTP_EPOCH_MIN) continue;

        float batv_avg = 0.0f, pvw_avg = 0.0f;
        int   cnt      = 0;
        xSemaphoreTake(s_hist_mutex, portMAX_DELAY);
        if (s_sample_cnt > 0) {
            batv_avg = s_batv_sum / (float)s_sample_cnt;
            pvw_avg  = s_pvw_sum  / (float)s_sample_cnt;
            cnt      = s_sample_cnt;
        }
        s_batv_sum   = 0.0f;
        s_pvw_sum    = 0.0f;
        s_sample_cnt = 0;
        xSemaphoreGive(s_hist_mutex);

        if (cnt == 0) continue;

        HistRecord rec;
        rec.ts    = (uint32_t)(now - HIST_INTERVAL_S);
        rec.bat_v = batv_avg;
        rec.pv_w  = pvw_avg;

        append_hist_record(&rec);
        remote_push_hist(&rec);

        struct tm tm_now;
        localtime_r(&now, &tm_now);
        cleanup_old_hist_files(tm_now.tm_year + 1900);
    }
}

/* ================================================================== */
/* ---- Endpoint GET /history                                     ---- */
/* ================================================================== */

struct HistBucket {
    float v_sum;   /* bat_v acumulado  */
    float i_sum;   /* pv_w acumulado   */
    int   count;
};

static esp_err_t history_handler(httpd_req_t *req)
{
    char view[8]    = "day";
    char ts_str[14] = "0";

    char qbuf[48] = {};
    if (httpd_req_get_url_query_str(req, qbuf, sizeof(qbuf)) == ESP_OK) {
        httpd_query_key_value(qbuf, "view", view,   sizeof(view));
        httpd_query_key_value(qbuf, "ts",   ts_str, sizeof(ts_str));
    }

    uint32_t ts_start = (uint32_t)strtoul(ts_str, nullptr, 10);
    if (ts_start == 0) {
        time_t now = time(nullptr);
        struct tm tm_now;
        gmtime_r(&now, &tm_now);
        uint32_t secs_today = (uint32_t)tm_now.tm_hour * 3600u
                            + (uint32_t)tm_now.tm_min  * 60u
                            + (uint32_t)tm_now.tm_sec;
        ts_start = (uint32_t)now - secs_today;
    }

    uint32_t period_s, bucket_s;
    int      n_buckets;
    if (strcmp(view, "week") == 0) {
        bucket_s = 3600;   n_buckets = 168; period_s = 7u * 86400u;
    } else if (strcmp(view, "month") == 0) {
        bucket_s = 86400;  n_buckets = 31;  period_s = 31u * 86400u;
    } else if (strcmp(view, "year") == 0) {
        bucket_s = 2635200; n_buckets = 12; period_s = 366u * 86400u;
    } else {
        bucket_s = 300;    n_buckets = 288; period_s = 86400u;
    }
    uint32_t ts_end = ts_start + period_s;

    HistBucket *buckets = (HistBucket *)calloc(n_buckets, sizeof(HistBucket));
    if (!buckets) { httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "OOM"); return ESP_FAIL; }

    struct tm tm1, tm2;
    time_t t1 = (time_t)ts_start, t2 = (time_t)(ts_end - 1);
    gmtime_r(&t1, &tm1); gmtime_r(&t2, &tm2);
    int year1 = tm1.tm_year + 1900, year2 = tm2.tm_year + 1900;

    for (int year = year1; year <= year2; year++) {
        char path[32];
        hist_file_path(year, path, sizeof(path));
        FILE *f = fopen(path, "rb");
        if (!f) continue;
        HistRecord recs[64];
        size_t n;
        while ((n = fread(recs, sizeof(HistRecord), 64, f)) > 0) {
            for (size_t k = 0; k < n; k++) {
                uint32_t rts = recs[k].ts;
                if (rts < ts_start || rts >= ts_end) continue;
                int bidx = (int)((rts - ts_start) / bucket_s);
                if (bidx < 0 || bidx >= n_buckets) continue;
                buckets[bidx].v_sum += recs[k].bat_v;
                buckets[bidx].i_sum += recs[k].pv_w;
                buckets[bidx].count++;
            }
        }
        fclose(f);
    }

    httpd_resp_set_type(req, "application/json");
    char hdr[128];
    int hlen = snprintf(hdr, sizeof(hdr),
        "{\"view\":\"%s\",\"ts_start\":%lu,\"bucket_s\":%lu,\"buckets\":[",
        view, (unsigned long)ts_start, (unsigned long)bucket_s);
    httpd_resp_send_chunk(req, hdr, hlen);

    bool first = true;
    for (int b = 0; b < n_buckets; b++) {
        if (buckets[b].count == 0) continue;
        float avg_v = buckets[b].v_sum / (float)buckets[b].count;
        float avg_p = buckets[b].i_sum / (float)buckets[b].count;
        uint32_t bts = ts_start + (uint32_t)b * bucket_s;
        char entry[96];
        int elen = snprintf(entry, sizeof(entry),
            "%s{\"ts\":%lu,\"v\":%.2f,\"p\":%.0f}",
            first ? "" : ",",
            (unsigned long)bts, avg_v, avg_p);
        httpd_resp_send_chunk(req, entry, elen);
        first = false;
    }
    httpd_resp_send_chunk(req, "]}", 2);
    httpd_resp_send_chunk(req, nullptr, 0);

    free(buckets);
    return ESP_OK;
}

/* ================================================================== */
/* ---- HTTP / WebSocket server                                   ---- */
/* ================================================================== */

struct ws_send_arg_t {
    httpd_handle_t hd;
    int            fd;
    char          *json;
    size_t         len;
};

static void ws_async_send_fn(void *arg)
{
    ws_send_arg_t *a = (ws_send_arg_t *)arg;
    httpd_ws_frame_t pkt = {};
    pkt.type    = HTTPD_WS_TYPE_TEXT;
    pkt.payload = (uint8_t *)a->json;
    pkt.len     = a->len;
    pkt.final   = true;
    esp_err_t r = httpd_ws_send_frame_async(a->hd, a->fd, &pkt);
    if (r != ESP_OK) {
        for (int i = 0; i < WS_MAX_CLIENTS; i++)
            if (s_ws_fds[i] == a->fd) { s_ws_fds[i] = -1; break; }
    }
    free(a->json);
    free(a);
}

/* ---- Tarea WebSocket push periódico (1 Hz) ---- */
static void ws_push_task(void *pvParameters)
{
    for (;;) {
        vTaskDelay(pdMS_TO_TICKS(1000));

        /* Acumular muestra para histórico */
        if (s_hist_mutex && s_inv_mutex) {
            xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
            bool  valid = (s_inv.valid & (1u << 0)) != 0;
            float batv  = s_inv.bat_v;
            float pvw   = (float)s_inv.pv_w;
            xSemaphoreGive(s_inv_mutex);
            if (valid) {
                xSemaphoreTake(s_hist_mutex, portMAX_DELAY);
                s_batv_sum   += batv;
                s_pvw_sum    += pvw;
                s_sample_cnt++;
                xSemaphoreGive(s_hist_mutex);
            }
        }

        if (!s_httpd) continue;
        char  *json = build_status_json();
        if (!json) continue;
        size_t len  = strlen(json);
        for (int i = 0; i < WS_MAX_CLIENTS; i++) {
            if (s_ws_fds[i] == -1) continue;
            ws_send_arg_t *a = (ws_send_arg_t *)malloc(sizeof(ws_send_arg_t));
            if (!a) continue;
            a->hd   = s_httpd;
            a->fd   = s_ws_fds[i];
            a->json = strdup(json);
            a->len  = len;
            if (!a->json ||
                httpd_queue_work(s_httpd, ws_async_send_fn, a) != ESP_OK) {
                if (a->json) free(a->json);
                free(a);
            }
        }
        free(json);
    }
}

static esp_err_t root_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req,
                    (const char *)inv_ui_html_start,
                    inv_ui_html_end - inv_ui_html_start);
    return ESP_OK;
}

static esp_err_t favicon_handler(httpd_req_t *req)
{
    httpd_resp_set_status(req, "204 No Content");
    httpd_resp_send(req, nullptr, 0);
    return ESP_OK;
}

static esp_err_t ws_handler(httpd_req_t *req)
{
    if (req->method == HTTP_GET) {
        int fd = httpd_req_to_sockfd(req);
        for (int i = 0; i < WS_MAX_CLIENTS; i++)
            if (s_ws_fds[i] == -1) { s_ws_fds[i] = fd; break; }
        ESP_LOGI(TAG, "WS cliente conectado fd=%d", fd);
        return ESP_OK;
    }
    /* Mensajes entrantes ignorados por ahora (solo monitoreo) */
    httpd_ws_frame_t pkt = {};
    uint8_t buf[64]      = {};
    pkt.payload = buf;
    httpd_ws_recv_frame(req, &pkt, sizeof(buf) - 1);
    return ESP_OK;
}

static void http_server_init(void)
{
    for (int i = 0; i < WS_MAX_CLIENTS; i++) s_ws_fds[i] = -1;

    httpd_config_t cfg   = HTTPD_DEFAULT_CONFIG();
    cfg.lru_purge_enable = true;
    cfg.max_open_sockets = WS_MAX_CLIENTS + 4;

    if (httpd_start(&s_httpd, &cfg) != ESP_OK) {
        ESP_LOGE(TAG, "Error iniciando HTTP server");
        return;
    }
    static const httpd_uri_t uri_root = {
        .uri                    = "/",
        .method                 = HTTP_GET,
        .handler                = root_handler,
        .user_ctx               = nullptr,
        .is_websocket           = false,
        .handle_ws_control_frames = false,
        .supported_subprotocol  = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_root);

    static const httpd_uri_t uri_ws = {
        .uri                    = "/ws",
        .method                 = HTTP_GET,
        .handler                = ws_handler,
        .user_ctx               = nullptr,
        .is_websocket           = true,
        .handle_ws_control_frames = false,
        .supported_subprotocol  = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_ws);

    static const httpd_uri_t uri_fav = {
        .uri                    = "/favicon.ico",
        .method                 = HTTP_GET,
        .handler                = favicon_handler,
        .user_ctx               = nullptr,
        .is_websocket           = false,
        .handle_ws_control_frames = false,
        .supported_subprotocol  = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_fav);

    static const httpd_uri_t uri_hist = {
        .uri                    = "/history",
        .method                 = HTTP_GET,
        .handler                = history_handler,
        .user_ctx               = nullptr,
        .is_websocket           = false,
        .handle_ws_control_frames = false,
        .supported_subprotocol  = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_hist);

    ESP_LOGI(TAG, "HTTP server iniciado");
    xTaskCreate(ws_push_task, "ws_push", 4096, nullptr, 4, nullptr);
}

/* ================================================================== */
/* ---- WiFi: provisioning (AP captive portal) + STA             ---- */
/* ================================================================== */

static const char CP_HTML[] =
"<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
"<title>Inversor Solar - WiFi Setup</title>"
"<style>"
"*{box-sizing:border-box}"
"body{font-family:sans-serif;max-width:420px;margin:30px auto;padding:16px;background:#0d1b2a;color:#e0e0e0}"
"h1{color:#f59e0b;text-align:center;margin-bottom:20px}"
"label{display:block;margin:10px 0 3px;color:#aaa;font-size:.9em}"
"input,select{width:100%;padding:9px;background:#1a2a3a;color:#eee;border:1px solid #2a4a6a;border-radius:5px;font-size:1em}"
".btn{display:block;width:100%;padding:11px;margin-top:8px;border:none;border-radius:5px;cursor:pointer;font-size:1em;font-weight:bold}"
".sec{background:#1a3a5a;color:#f59e0b}.pri{background:#f59e0b;color:#000;margin-top:14px}"
"#st{text-align:center;margin-top:14px;min-height:18px;color:#aaa}"
"</style></head><body>"
"<h1>Inversor Solar<br><small style=\"font-size:.5em;color:#888\">WiFi Setup</small></h1>"
"<label>Red WiFi (SSID)</label>"
"<input id=\"sid\" type=\"text\" placeholder=\"Nombre de la red\" required>"
"<label>Contrase&#xF1;a</label>"
"<input id=\"pwd\" type=\"password\" placeholder=\"Vac&#xED;o si es red abierta\">"
"<button class=\"btn sec\" onclick=\"scan()\">&#128246; Escanear redes</button>"
"<select id=\"nets\" style=\"display:none;margin-top:5px\""
" onchange=\"if(this.value)document.getElementById('sid').value=this.value\">"
"<option value=\"\">-- Seleccionar --</option></select>"
"<button class=\"btn pri\" onclick=\"save()\">&#10003; Guardar y reiniciar</button>"
"<div id=\"st\"></div>"
"<script>"
"function st(m,c){var e=document.getElementById('st');e.textContent=m;e.style.color=c||'#aaa';}"
"function scan(){"
"  st('Escaneando...');"
"  fetch('/wifi/scan').then(function(r){return r.json();}).then(function(d){"
"    var s=document.getElementById('nets');"
"    s.innerHTML='<option value=\"\">-- Seleccionar --</option>';"
"    d.forEach(function(n){var o=document.createElement('option');o.textContent=n;s.appendChild(o);});"
"    s.style.display='block';st(d.length+' redes encontradas');"
"  }).catch(function(){st('Error al escanear','#f66');});"
"}"
"function save(){"
"  var s=document.getElementById('sid').value.trim();"
"  if(!s){st('Introduce un SSID','#f66');return;}"
"  st('Guardando...');"
"  fetch('/wifi/save',{method:'POST',headers:{'Content-Type':'application/json'},"
"    body:JSON.stringify({ssid:s,pass:document.getElementById('pwd').value})})"
"  .then(function(r){return r.text();}).then(function(t){st(t,'#0d0');})"
"  .catch(function(){st('Error guardando','#f66');});"
"}"
"</script></body></html>";

static bool wifi_credentials_load(char *ssid, size_t ssid_len,
                                   char *pass, size_t pass_len)
{
    nvs_handle_t h;
    if (nvs_open(NVS_WIFI_NS, NVS_READONLY, &h) != ESP_OK) return false;
    bool ok = (nvs_get_str(h, NVS_WIFI_SSID, ssid, &ssid_len) == ESP_OK &&
               nvs_get_str(h, NVS_WIFI_PASS, pass, &pass_len) == ESP_OK &&
               ssid[0] != '\0');
    nvs_close(h);
    return ok;
}

static void wifi_credentials_save(const char *ssid, const char *pass)
{
    nvs_handle_t h;
    if (nvs_open(NVS_WIFI_NS, NVS_READWRITE, &h) != ESP_OK) return;
    nvs_set_str(h, NVS_WIFI_SSID, ssid);
    nvs_set_str(h, NVS_WIFI_PASS, pass ? pass : "");
    nvs_commit(h);
    nvs_close(h);
    ESP_LOGI(TAG_WIFI, "Credenciales WiFi guardadas: '%s'", ssid);
}

static void dns_server_task(void *pvParameters)
{
    const uint8_t ap_ip[4] = {192, 168, 4, 1};
    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock < 0) { vTaskDelete(NULL); return; }
    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(53);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock); vTaskDelete(NULL); return;
    }
    uint8_t buf[512], resp[532];
    for (;;) {
        struct sockaddr_in client = {};
        socklen_t clen = sizeof(client);
        int len = recvfrom(sock, buf, sizeof(buf) - 1, 0,
                           (struct sockaddr *)&client, &clen);
        if (len < 12 || len > (int)(sizeof(resp) - 20)) continue;
        memcpy(resp, buf, (size_t)len);
        resp[2] = 0x81; resp[3] = 0x80;
        resp[6] = 0x00; resp[7] = 0x01;
        resp[8] = resp[9] = resp[10] = resp[11] = 0x00;
        int rlen = len;
        resp[rlen++] = 0xC0; resp[rlen++] = 0x0C;
        resp[rlen++] = 0x00; resp[rlen++] = 0x01;
        resp[rlen++] = 0x00; resp[rlen++] = 0x01;
        resp[rlen++] = 0x00; resp[rlen++] = 0x00;
        resp[rlen++] = 0x00; resp[rlen++] = 0x0A;
        resp[rlen++] = 0x00; resp[rlen++] = 0x04;
        resp[rlen++] = ap_ip[0]; resp[rlen++] = ap_ip[1];
        resp[rlen++] = ap_ip[2]; resp[rlen++] = ap_ip[3];
        sendto(sock, resp, (size_t)rlen, 0, (struct sockaddr *)&client, clen);
    }
}

static esp_err_t wifi_setup_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, CP_HTML, (ssize_t)strlen(CP_HTML));
    return ESP_OK;
}

static esp_err_t wifi_scan_handler(httpd_req_t *req)
{
    wifi_scan_config_t scfg = {};
    scfg.show_hidden = false;
    esp_wifi_scan_start(&scfg, true);
    uint16_t count = 20;
    wifi_ap_record_t records[20];
    memset(records, 0, sizeof(records));
    esp_wifi_scan_get_ap_records(&count, records);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < (int)count; i++)
        if (records[i].ssid[0] != '\0')
            cJSON_AddItemToArray(arr, cJSON_CreateString((char *)records[i].ssid));
    char *json = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, json, (ssize_t)strlen(json));
    free(json);
    return ESP_OK;
}

static esp_err_t wifi_save_handler(httpd_req_t *req)
{
    char buf[384] = {};
    int  received = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (received <= 0) { httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Bad Request"); return ESP_FAIL; }
    buf[received] = '\0';
    cJSON *json   = cJSON_Parse(buf);
    cJSON *ssid_j = json ? cJSON_GetObjectItem(json, "ssid") : nullptr;
    cJSON *pass_j = json ? cJSON_GetObjectItem(json, "pass") : nullptr;
    if (!ssid_j || !cJSON_IsString(ssid_j) || ssid_j->valuestring[0] == '\0') {
        if (json) cJSON_Delete(json);
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "SSID requerido");
        return ESP_FAIL;
    }
    const char *ssid = ssid_j->valuestring;
    const char *pass = (pass_j && cJSON_IsString(pass_j)) ? pass_j->valuestring : "";
    wifi_credentials_save(ssid, pass);
    cJSON_Delete(json);
    static const char ok_msg[] = "Credenciales guardadas. Reiniciando en 2 segundos...";
    httpd_resp_set_type(req, "text/plain; charset=utf-8");
    httpd_resp_send(req, ok_msg, (ssize_t)sizeof(ok_msg) - 1);
    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
    return ESP_OK;
}

static esp_err_t captive_404_handler(httpd_req_t *req, httpd_err_code_t err)
{
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "http://192.168.4.1/");
    httpd_resp_send(req, NULL, 0);
    return ESP_OK;
}

static void http_captive_server_init(void)
{
    httpd_config_t cfg   = HTTPD_DEFAULT_CONFIG();
    cfg.lru_purge_enable = true;
    cfg.max_open_sockets = AP_MAX_CONN + 2;
    if (httpd_start(&s_httpd, &cfg) != ESP_OK) {
        ESP_LOGE(TAG, "Error iniciando HTTP captive portal");
        return;
    }
    static const httpd_uri_t uri_root = {
        .uri = "/", .method = HTTP_GET,
        .handler = wifi_setup_handler, .user_ctx = nullptr,
        .is_websocket = false, .handle_ws_control_frames = false, .supported_subprotocol = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_root);
    static const httpd_uri_t uri_scan = {
        .uri = "/wifi/scan", .method = HTTP_GET,
        .handler = wifi_scan_handler, .user_ctx = nullptr,
        .is_websocket = false, .handle_ws_control_frames = false, .supported_subprotocol = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_scan);
    static const httpd_uri_t uri_save = {
        .uri = "/wifi/save", .method = HTTP_POST,
        .handler = wifi_save_handler, .user_ctx = nullptr,
        .is_websocket = false, .handle_ws_control_frames = false, .supported_subprotocol = nullptr,
    };
    httpd_register_uri_handler(s_httpd, &uri_save);
    httpd_register_err_handler(s_httpd, HTTPD_404_NOT_FOUND, captive_404_handler);
    ESP_LOGI(TAG, "Captive portal activo en http://192.168.4.1/");
}

static void wifi_start_provisioning(void)
{
    wifi_config_t ap_cfg = {};
    memcpy(ap_cfg.ap.ssid, AP_SSID, strlen(AP_SSID));
    ap_cfg.ap.ssid_len       = (uint8_t)strlen(AP_SSID);
    ap_cfg.ap.channel        = AP_CHANNEL;
    ap_cfg.ap.max_connection = AP_MAX_CONN;
    ap_cfg.ap.authmode       = WIFI_AUTH_OPEN;
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_APSTA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());
    xTaskCreate(dns_server_task, "dns_srv", 4096, nullptr, 5, nullptr);
    http_captive_server_init();
    ESP_LOGI(TAG_WIFI, "Provisioning: conecta a '%s' → http://192.168.4.1/", AP_SSID);
}

/* ================================================================== */
/* ---- WiFi STA                                                  ---- */
/* ================================================================== */

static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                               int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        if (s_wifi_retry_count < WIFI_RETRY_MAX) {
            esp_wifi_connect();
            s_wifi_retry_count++;
            ESP_LOGW(TAG_WIFI, "Reintentando (%d/%d)...", s_wifi_retry_count, WIFI_RETRY_MAX);
        } else {
            if (s_wifi_event_group)
                xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG_WIFI, "IP: " IPSTR, IP2STR(&event->ip_info.ip));
        if (s_inv_mutex) {
            xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
            snprintf(s_inv.wifi_ip, sizeof(s_inv.wifi_ip),
                     IPSTR, IP2STR(&event->ip_info.ip));
            xSemaphoreGive(s_inv_mutex);
        }
        s_wifi_retry_count = 0;
        if (s_wifi_event_group)
            xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static bool wifi_try_sta(const char *ssid, const char *pass)
{
    s_wifi_event_group = xEventGroupCreate();
    s_wifi_retry_count = 0;
    esp_event_handler_instance_t instance_any_id;
    esp_event_handler_instance_t instance_got_ip;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, nullptr, &instance_any_id));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, nullptr, &instance_got_ip));
    wifi_config_t wifi_config = {};
    strncpy((char *)wifi_config.sta.ssid,     ssid, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, pass, sizeof(wifi_config.sta.password) - 1);
    wifi_config.sta.threshold.authmode =
        (pass[0] != '\0') ? WIFI_AUTH_WPA2_PSK : WIFI_AUTH_OPEN;
    wifi_config.sta.pmf_cfg.capable  = true;
    wifi_config.sta.pmf_cfg.required = false;
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_LOGI(TAG_WIFI, "Conectando a '%s'...", ssid);
    EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group,
                                           WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
                                           pdFALSE, pdFALSE, pdMS_TO_TICKS(15000));
    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG_WIFI, "Conectado a '%s'", ssid);
        return true;
    }
    ESP_LOGW(TAG_WIFI, "No se pudo conectar a '%s'", ssid);
    esp_event_handler_instance_unregister(WIFI_EVENT, ESP_EVENT_ANY_ID,  instance_any_id);
    esp_event_handler_instance_unregister(IP_EVENT, IP_EVENT_STA_GOT_IP, instance_got_ip);
    esp_wifi_stop();
    vEventGroupDelete(s_wifi_event_group);
    s_wifi_event_group = nullptr;
    return false;
}

/* ================================================================== */
/* ---- Tarea principal: poll PI30 por UART                       ---- */
/* ================================================================== */

static void inv_uart_task(void *pvParameters)
{
    uart_inv_init();

    char payload[288];

    /* Primera lectura inmediata */
    if (send_pi30_cmd("QMOD",  payload, sizeof(payload)) > 0) parse_qmod(payload);
    if (send_pi30_cmd("QPIGS", payload, sizeof(payload)) > 0) parse_qpigs(payload);
    if (send_pi30_cmd("QPIWS", payload, sizeof(payload)) > 0) parse_qpiws(payload);

    uint64_t last_qpigs  = millis();
    uint64_t last_qmod   = millis();
    uint64_t last_qpiws  = millis();

    for (;;) {
        uint64_t now = millis();

        if (now - last_qpigs >= POLL_QPIGS_MS) {
            if (send_pi30_cmd("QPIGS", payload, sizeof(payload)) > 0) {
                parse_qpigs(payload);
            } else {
                xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
                s_inv.uart_ok = false;
                xSemaphoreGive(s_inv_mutex);
                ESP_LOGW(TAG, "QPIGS sin respuesta");
            }
            last_qpigs = millis();
        }

        if (now - last_qmod >= POLL_QMOD_MS) {
            if (send_pi30_cmd("QMOD", payload, sizeof(payload)) > 0)
                parse_qmod(payload);
            last_qmod = millis();
        }

        if (now - last_qpiws >= POLL_QPIWS_MS) {
            if (send_pi30_cmd("QPIWS", payload, sizeof(payload)) > 0)
                parse_qpiws(payload);
            last_qpiws = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

/* ================================================================== */
/* ---- Tarea push remoto                                         ---- */
/* ================================================================== */

static void remote_push_task(void *pvParameters)
{
    for (;;) {
        vTaskDelay(pdMS_TO_TICKS(REMOTE_PUSH_MS));

        if (s_inv_mutex) {
            xSemaphoreTake(s_inv_mutex, portMAX_DELAY);
            bool has_ip = (s_inv.wifi_ip[0] != '\0');
            xSemaphoreGive(s_inv_mutex);
            if (!has_ip) continue;
        }

        char *json = build_status_json();
        if (!json) continue;

        esp_http_client_config_t cfg = {};
        cfg.url               = REMOTE_PUSH_URL;
        cfg.method            = HTTP_METHOD_POST;
        cfg.crt_bundle_attach = esp_crt_bundle_attach;
        cfg.timeout_ms        = 8000;

        esp_http_client_handle_t client = esp_http_client_init(&cfg);
        esp_http_client_set_header(client, "Content-Type", "application/json");
        esp_http_client_set_header(client, "X-API-Key",    REMOTE_API_KEY);
        esp_http_client_set_post_field(client, json, (int)strlen(json));

        esp_err_t err = esp_http_client_perform(client);
        if (err == ESP_OK) {
            int status = esp_http_client_get_status_code(client);
            if (status == 200) ESP_LOGI(TAG, "Push remoto OK");
            else               ESP_LOGW(TAG, "Push remoto: HTTP %d", status);
        } else {
            ESP_LOGW(TAG, "Push remoto falló: %s", esp_err_to_name(err));
        }
        esp_http_client_cleanup(client);
        free(json);
    }
}

/* ================================================================== */
/* ---- Punto de entrada                                          ---- */
/* ================================================================== */

extern "C" void app_main(void)
{
    /* NVS requerido por WiFi */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    s_inv_mutex  = xSemaphoreCreateMutex();
    s_hist_mutex = xSemaphoreCreateMutex();
    configASSERT(s_inv_mutex && s_hist_mutex);

    spiffs_init();

    esp_log_level_set("httpd_txrx", ESP_LOG_ERROR);
    esp_log_level_set("httpd_ws",   ESP_LOG_ERROR);

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();
    esp_netif_create_default_wifi_ap();

    wifi_init_config_t wcfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&wcfg));

    char saved_ssid[64] = {};
    char saved_pass[64] = {};
    bool has_creds = wifi_credentials_load(saved_ssid, sizeof(saved_ssid),
                                           saved_pass, sizeof(saved_pass));
    bool wifi_ok = false;
    if (has_creds) {
        ESP_LOGI(TAG_WIFI, "Credenciales guardadas: '%s'", saved_ssid);
        wifi_ok = wifi_try_sta(saved_ssid, saved_pass);
    }

    if (!wifi_ok) {
        wifi_start_provisioning();
        while (1) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    sntp_init_fn();
    http_server_init();

    mdns_init();
    mdns_hostname_set("inversor");
    mdns_instance_name_set("Inversor Solar DT4862-BF");
    mdns_service_add(nullptr, "_http", "_tcp", 80, nullptr, 0);
    ESP_LOGI(TAG, "mDNS: http://inversor.local/");

    xTaskCreate(inv_uart_task,    "inv_uart",    8192, nullptr, 5, nullptr);
    xTaskCreate(remote_push_task, "remote_push", 8192, nullptr, 3, nullptr);
    xTaskCreate(hist_task,        "hist",        4096, nullptr, 2, nullptr);

    while (1) vTaskDelay(pdMS_TO_TICKS(1000));
}
