# Datouboss DT4862-BF Inverter Monitor

Monitor en tiempo real para el inversor solar **Datouboss DT4862-BF** (y compatibles con protocolo PI30), basado en **ESP32-S3** con ESP-IDF v5.x.

## Características

- **Comunicación RS232 / PI30** — consulta periódica de comandos `QPIGS`, `QMOD` y `QPIWS`.
- **WiFi STA** con portal de aprovisionamiento AP (`INV-Setup`) si no hay credenciales guardadas en NVS.
- **Servidor HTTP + WebSocket** embebido — interfaz web servida desde flash (archivo `inv_ui.html` embebido).
- **Histórico en SPIFFS** — registros binarios cada 5 minutos (tensión de batería y potencia PV).
- **Push remoto** — envío de datos en tiempo real e histórico a servidor externo vía HTTPS.
- **mDNS** — el dispositivo es accesible como `inversor.local` en la red local.
- **SNTP** — sincronización de hora para timestamping correcto de los registros.

## Hardware

| Componente | Detalle |
|---|---|
| MCU | ESP32-S3 (16 MB flash) |
| Interfaz serial | MAX3232 (RS232 ↔ UART TTL) |
| TX GPIO | 17 (configurable) |
| RX GPIO | 18 (configurable) |
| Baud rate | 2400 bps (configurable, PI30 estándar) |

### Conexión RS232

```
Inversor DB9  ──►  MAX3232  ──►  ESP32-S3
    TX  ────────────────────────►  GPIO RX (18)
    RX  ◄────────────────────────  GPIO TX (17)
    GND ────────────────────────── GND
```

## Tabla de particiones (16 MB)

| Nombre | Tipo | Offset | Tamaño |
|---|---|---|---|
| nvs | data/nvs | 0x9000 | 24 KB |
| phy_init | data/phy | 0xF000 | 4 KB |
| factory | app/factory | 0x10000 | 3 MB |
| storage (SPIFFS) | data/spiffs | 0x310000 | ~12.9 MB |

## Compilación y flash

### Requisitos

- [ESP-IDF v5.x](https://docs.espressif.com/projects/esp-idf/en/stable/esp32s3/get-started/index.html)
- Target: `esp32s3`

### Pasos

```bash
# Configurar target (solo la primera vez)
idf.py set-target esp32s3

# Ajustar parámetros opcionales (UART port, GPIOs, baud rate)
idf.py menuconfig   # menú "Inverter Monitor"

# Compilar
idf.py build

# Flash + monitor serie
idf.py -p /dev/ttyUSB0 flash monitor
```

## Configuración (menuconfig)

| Opción | Por defecto | Descripción |
|---|---|---|
| `INV_UART_PORT_NUM` | 1 | Puerto UART (0-2) |
| `INV_UART_TX_GPIO` | 17 | GPIO TX hacia MAX3232 |
| `INV_UART_RX_GPIO` | 18 | GPIO RX desde MAX3232 |
| `INV_UART_BAUD` | 2400 | Velocidad RS232 |

## Aprovisionamiento WiFi

1. Al arrancar sin credenciales, el ESP32-S3 crea el AP **`INV-Setup`**.
2. Conéctate y abre `http://192.168.4.1` para introducir el SSID y contraseña de tu red.
3. Las credenciales se guardan en NVS y el dispositivo se reconecta automáticamente.

## Interfaz web

Una vez conectado a la red, accede a:

- `http://inversor.local` (mDNS)
- `http://<IP asignada>`

La interfaz muestra en tiempo real (WebSocket):

- Tensión, frecuencia y potencia de entrada AC
- Estado y capacidad de la batería
- Potencia y tensión del panel PV
- Temperatura del disipador
- Modo de operación y flags de advertencia
- Gráficas históricas de batería y PV (SPIFFS)

## Estructura del proyecto

```
main/
  main.cpp          — firmware principal (FreeRTOS, PI30, HTTP/WS, SPIFFS)
  inv_ui.html       — interfaz web embebida
  Kconfig.projbuild — opciones de menuconfig
inv/                — scripts PHP del servidor remoto
  data.php          — lectura de datos en tiempo real
  update.php        — recepción push del ESP32
  data_history.php  — lectura de histórico
  update_history.php— recepción histórico del ESP32
partitions.csv      — tabla de particiones personalizada (16 MB)
```

## Licencia

CC0 1.0 Universal — ver cabecera de `main.cpp`.
