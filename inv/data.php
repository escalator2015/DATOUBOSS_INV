<?php
/**
 * data.php — Sirve el último JSON del inversor al navegador (solo lectura).
 * Subir a: https://luaresearch.es/inv/data.php
 */

define('DATA_FILE', __DIR__ . '/inv_data.json');
define('STALE_SEC', 30);

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

if (!file_exists(DATA_FILE)) {
    echo '{"error":"no_data"}';
    exit;
}

$age  = time() - filemtime(DATA_FILE);
$json = file_get_contents(DATA_FILE);
$data = json_decode($json, true);

if (!is_array($data)) {
    http_response_code(500);
    echo '{"error":"corrupt"}';
    exit;
}

$data['stale'] = ($age > STALE_SEC);
$data['age_s'] = $age;

echo json_encode($data, JSON_UNESCAPED_UNICODE);
