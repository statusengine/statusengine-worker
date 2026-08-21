<?php

// Execute RAW Naemon external commands
//
// Every raw command MUST begin with "[<unix timestamp>] ". Naemon's
// command_parse() reads the entry time first and rejects anything without it
// with "Commands must begin with a timestamp inside square brackets" - it logs
// that into naemon.log and tells nobody else, so a command without the prefix
// simply never happens. (The worker's HTTP command API adds the prefix for you
// if it is missing; a direct Gearman/AMQP publisher like this one must not
// forget it.)
$payload = [
    'Command' => 'raw',
    'Data'    => sprintf('[%s] SCHEDULE_FORCED_SVC_CHECK;%s;%s;%s', time(), 'hostname', 'service name', time())
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));

$payload = [
    'Command' => 'raw',
    'Data'    => sprintf('[%s] ENABLE_HOST_FLAP_DETECTION;%s', time(), 'hostname')
];

$Client = new \GearmanClient();
$Client->addServer('127.0.0.1', 4730);
$Client->doBackground('statusngin_cmd', json_encode($payload));


//SEND_CUSTOM_SVC_NOTIFICATION
$payload = [
    'Command' => 'raw',
    'Data'    => sprintf(
        '[%s] SEND_CUSTOM_SVC_NOTIFICATION;%s;%s;%s;%s;%s',
        time(),
        'hostname',
        'service name',
        0, // type (0 = default, 1 = broadcast, 2 = forced, 3 = broadcast and forced)
        'author',
        'comment'
    )
];