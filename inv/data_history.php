<?php
/**
 * data_history.php — Sirve histórico agregado del inversor al navegador.
 * Parámetros GET:
 *   view  = day | week | month | year  (defecto: day)
 *   ts    = Unix timestamp inicio del período  (defecto: inicio del día UTC)
 *
 * Responde JSON compatible con el endpoint /history del ESP32:
 * { "view": "...", "ts_start": N, "bucket_s": N, "buckets": [{ts, v, p}, ...] }
 *   v = tensión batería promedio (V)
 *   p = potencia PV promedio (W)
 */

define('DATA_DIR', __DIR__ . '/');

$view     = $_GET['view'] ?? 'day';
$ts_start = (int)($_GET['ts'] ?? 0);

if (!in_array($view, ['day', 'week', 'month', 'year'], true)) {
    $view = 'day';
}

switch ($view) {
    case 'week':
        $bucket_s  = 3600;
        $n_buckets = 168;
        $period_s  = 7 * 86400;
        break;
    case 'month':
        $bucket_s  = 86400;
        $n_buckets = 31;
        $period_s  = 31 * 86400;
        break;
    case 'year':
        $bucket_s  = 2635200;
        $n_buckets = 12;
        $period_s  = 366 * 86400;
        break;
    default: /* day */
        $bucket_s  = 300;
        $n_buckets = 288;
        $period_s  = 86400;
        break;
}

if ($ts_start === 0) {
    $ts_start = mktime(0, 0, 0, (int)gmdate('n'), (int)gmdate('j'), (int)gmdate('Y'));
}

$ts_end = $ts_start + $period_s;

$buckets = array_fill(0, $n_buckets, ['v_sum' => 0.0, 'p_sum' => 0.0, 'count' => 0]);

$year1 = (int)gmdate('Y', $ts_start);
$year2 = (int)gmdate('Y', $ts_end - 1);

for ($year = $year1; $year <= $year2; $year++) {
    $file = DATA_DIR . 'hist_' . $year . '.jsonl';
    if (!file_exists($file)) continue;
    $fp = fopen($file, 'r');
    if (!$fp) continue;
    while (($line = fgets($fp)) !== false) {
        $rec = json_decode($line, true);
        if (!$rec || !isset($rec['ts'])) continue;
        $rts = (int)$rec['ts'];
        if ($rts < $ts_start || $rts >= $ts_end) continue;
        $bidx = (int)(($rts - $ts_start) / $bucket_s);
        if ($bidx < 0 || $bidx >= $n_buckets) continue;
        $buckets[$bidx]['v_sum'] += (float)($rec['v'] ?? 0);
        $buckets[$bidx]['p_sum'] += (float)($rec['p'] ?? 0);
        $buckets[$bidx]['count']++;
    }
    fclose($fp);
}

$result_buckets = [];
for ($b = 0; $b < $n_buckets; $b++) {
    if ($buckets[$b]['count'] === 0) continue;
    $cnt = $buckets[$b]['count'];
    $result_buckets[] = [
        'ts' => $ts_start + $b * $bucket_s,
        'v'  => round($buckets[$b]['v_sum'] / $cnt, 2),
        'p'  => round($buckets[$b]['p_sum'] / $cnt, 0),
    ];
}

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

echo json_encode([
    'view'      => $view,
    'ts_start'  => $ts_start,
    'bucket_s'  => $bucket_s,
    'buckets'   => $result_buckets,
], JSON_UNESCAPED_UNICODE);
