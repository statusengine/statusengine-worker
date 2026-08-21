<?php

$results = [
    'messages' => []
];

foreach ($config['checks'] as $pluginConfig) {
    // Create bulk message for Statusengine Broker
    $results['messages'][] = [
        'Command' => 'check_result',
        'Data'    => [
            'host_name'           => 'hostname',
            'service_description' => 'service_description',
            'output'              => 'Naemon Short plugin output',
            'long_output'         => 'Naemon long plugin output or empty string',
            // Perfdata has its own field. Appending it to 'output' after a pipe
            // works too, but the broker assembles output/long_output/perf_data
            // into Naemon's format itself when you keep them apart.
            'perf_data'           => 'performance_data=1',
            'check_type'          => 1, //https://github.com/naemon/naemon-core/blob/cec6e10cbee9478de04b4cf5af29e83d47b5cfd9/src/naemon/common.h#L330-L334
            'return_code'         => 0, // Naemon Exit Code 0=Ok, 1=warning, 2=Critical, 3=Unknown
            'start_time'          => time(), // current timestamp
            'end_time'            => time(), // current timestamp
            'early_timeout'       => 0,
            'latency'             => 0,
            'exited_ok'           => 1
        ]
    ];
}

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($results));