<?php

// Delete host downtime

$payload = [
    'Command' => 'delete_downtime',
    'Data'    => [
        'host_name'  => 'hostname',
        'start_time' => $Downtime->getScheduledStartTime(), // unixtimestamp
        'end_time'   => $Downtime->getScheduledEndTime(), // unixtimestamp
        'comment'    => $Downtime->getCommentData() // string
    ]
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));


// Delete service downtime

$payload = [
    'Command' => 'delete_downtime',
    'Data'    => [
        'host_name'  => 'hostname',
        'service_description' => 'service name',
        'start_time' => $Downtime->getScheduledStartTime(), // unixtimestamp
        'end_time'   => $Downtime->getScheduledEndTime(), // unixtimestamp
        'comment'    => $Downtime->getCommentData() // string
    ]
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));
