<?php
/**
 * update_history.php — Recibe una muestra histórica del ESP32-S3 (cada 5 min).
 * Payload JSON: { "ts": <epoch>, "v": <bat_v en V>, "p": <pv_w en W> }
 * Subir a: https://luaresearch.es/inv/update_history.php
 */

define('API_KEY',  'b7e4a1f3c9d2e8b6a0f5c3d7e2b9a4f1c6d0e3b8a5f2c7d1e6b3a0f8c4d9e2b5');
define('DATA_DIR', __DIR__ . '/');
define('MAX_BODY', 256);

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    exit;
}

$headers = array_change_key_case(getallheaders(), CASE_LOWER);
if (($headers['x-api-key'] ?? '') !== API_KEY) {
    http_response_code(403);
    exit('Forbidden');
}

$body = file_get_contents('php://input', false, null, 0, MAX_BODY);
if (!$body) {
    http_response_code(400);
    exit('Empty body');
}

$data = json_decode($body, true);
if (!is_array($data) || !isset($data['ts'], $data['v'], $data['p'])) {
    http_response_code(400);
    exit('Bad JSON — required: ts, v, p');
}

$ts = (int)$data['ts'];
$v  = (float)$data['v'];   // tensión batería (V)
$p  = (float)$data['p'];   // potencia PV (W)

// Rangos razonables para un inversor 48V con panel solar
if ($ts < 1577836800 || $v < 0 || $v > 100 || $p < 0 || $p > 20000) {
    http_response_code(400);
    exit('Out of range');
}

$year = (int)date('Y', $ts);
$file = DATA_DIR . 'hist_' . $year . '.jsonl';

$line = json_encode(['ts' => $ts, 'v' => $v, 'p' => $p]) . "\n";
$fp = fopen($file, 'a');
if ($fp) {
    flock($fp, LOCK_EX);
    fwrite($fp, $line);
    flock($fp, LOCK_UN);
    fclose($fp);
    http_response_code(200);
    echo 'OK';
} else {
    http_response_code(500);
    echo 'Write error';
}
