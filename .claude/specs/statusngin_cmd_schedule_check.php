<?php

// Schedule host check

$payload = [
    'Command' => 'schedule_check',
    'Data'    => [
        'host_name'  => 'hostname',
        'schedule_time'       => time(), // unixtimestamp
    ]
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));


// Schedule service check

$payload = [
    'Command' => 'schedule_check',
    'Data'    => [
        'host_name'  => 'hostname',
        'service_description' => 'service name',
        'schedule_time'       => time(), // unixtimestamp
    ]
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));
