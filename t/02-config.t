use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Config;

# Defaults
my $conf = AIO::Celery::Config->new;
is($conf->task_default_queue, 'celery',                              'default queue');
is($conf->broker_url,         'amqp://guest:guest@localhost:5672//', 'default broker');
is($conf->task_ignore_result, 0,                                     'default ignore_result');
is($conf->result_expires,     86400,                                 'default result_expires');
is($conf->worker_prefetch_multiplier, 4,                             'default prefetch');
is($conf->result_backend,     undef,                                 'default result_backend is undef');

# Update with valid keys
$conf->update(
    task_default_queue => 'myqueue',
    broker_url         => 'amqp://user:pass@rabbit:5672//',
    result_backend     => 'redis://localhost:6379/1',
);
is($conf->task_default_queue, 'myqueue',                          'updated queue');
is($conf->broker_url,         'amqp://user:pass@rabbit:5672//',  'updated broker');
is($conf->result_backend,     'redis://localhost:6379/1',         'updated result_backend');

# Update with unknown key
like(
    dies { $conf->update(unknown_key => 'value') },
    qr/Unknown configuration key: unknown_key/,
    'update rejects unknown key',
);

done_testing;
