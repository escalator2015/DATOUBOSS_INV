<?php
/**
 * update.php — Recibe el JSON del ESP32-S3 (estado inversor) y lo guarda en disco.
 * Subir a: https://luaresearch.es/inv/update.php
 */

define('API_KEY',   'b7e4a1f3c9d2e8b6a0f5c3d7e2b9a4f1c6d0e3b8a5f2c7d1e6b3a0f8c4d9e2b5');
define('DATA_FILE', __DIR__ . '/inv_data.json');
define('MAX_BODY',  8192);

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
if (!is_array($data)) {
    http_response_code(400);
    exit('Bad JSON');
}

$data['server_ts'] = time();

$tmp = DATA_FILE . '.tmp';
if (file_put_contents($tmp, json_encode($data, JSON_UNESCAPED_UNICODE), LOCK_EX) !== false) {
    rename($tmp, DATA_FILE);
    http_response_code(200);
    echo 'OK';
} else {
    http_response_code(500);
    echo 'Write error';
}
