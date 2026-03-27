use strict;
use warnings;
use Test2::V0;
use JSON::MaybeXS qw(decode_json);
use AIO::Celery::AMQP qw(create_task_message);

# Basic message creation
my $msg = create_task_message(
    task_id   => 'abc-123-def',
    task_name => 'tasks.add',
    args      => [2, 3],
    kwargs    => { extra => 'val' },
);

# Structure
ref_ok($msg, 'HASH', 'message is hashref');
ok(exists $msg->{body},    'has body');
ok(exists $msg->{headers}, 'has headers');
is($msg->{content_type},     'application/json', 'content_type');
is($msg->{content_encoding}, 'utf-8',            'content_encoding');
is($msg->{delivery_mode},    2,                   'delivery_mode persistent');
is($msg->{correlation_id},   'abc-123-def',       'correlation_id matches task_id');

# Headers
my $h = $msg->{headers};
is($h->{id},   'abc-123-def', 'header id');
is($h->{task}, 'tasks.add',   'header task');
is($h->{lang}, 'py',          'header lang is py for compat');
is($h->{retries}, 0,          'header retries default 0');
is($h->{root_id}, 'abc-123-def', 'root_id defaults to task_id');
is($h->{parent_id}, undef,       'parent_id default undef');
is($h->{eta},       undef,       'eta default undef');
ref_ok($h->{stamps}, 'HASH',     'stamps is hash');
ref_ok($h->{timelimit}, 'ARRAY', 'timelimit is array');
is(scalar @{$h->{timelimit}}, 2, 'timelimit has 2 elements');

# Body: JSON-decoded should be [args, kwargs, {callbacks, errbacks, chain, chord}]
my $body = decode_json($msg->{body});
ref_ok($body, 'ARRAY', 'body is array');
is(scalar @$body, 3, 'body has 3 elements');
is($body->[0], [2, 3],              'body[0] = args');
is($body->[1], { extra => 'val' },  'body[1] = kwargs');
is($body->[2]{callbacks}, undef,     'body[2] callbacks');
is($body->[2]{errbacks},  undef,     'body[2] errbacks');
is($body->[2]{chord},     undef,     'body[2] chord');
ref_ok($body->[2]{chain}, 'ARRAY',  'body[2] chain is array');

# ignore_result as JSON boolean
ok(JSON::MaybeXS::is_bool($h->{ignore_result}), 'ignore_result is JSON bool');
ok(!$h->{ignore_result}, 'ignore_result default false');

# With ignore_result = 1
my $msg2 = create_task_message(
    task_id       => 'xyz-456',
    task_name     => 'tasks.noresult',
    ignore_result => 1,
);
ok($msg2->{headers}{ignore_result}, 'ignore_result true when set');

# With priority
my $msg3 = create_task_message(
    task_id   => 'pri-789',
    task_name => 'tasks.pri',
    priority  => 5,
);
is($msg3->{priority}, 5, 'priority set');

# With countdown (ETA)
my $msg4 = create_task_message(
    task_id   => 'eta-101',
    task_name => 'tasks.delayed',
    countdown => 60,
);
ok(defined $msg4->{headers}{eta}, 'eta set when countdown given');
like($msg4->{headers}{eta}, qr/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, 'eta is ISO format');

# With parent_id and root_id
my $msg5 = create_task_message(
    task_id   => 'child-1',
    task_name => 'tasks.child',
    parent_id => 'parent-1',
    root_id   => 'root-1',
);
is($msg5->{headers}{parent_id}, 'parent-1', 'parent_id set');
is($msg5->{headers}{root_id},   'root-1',   'root_id set');

# Auto-generated task_id
my $msg6 = create_task_message(task_name => 'tasks.auto');
like($msg6->{headers}{id}, qr/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    'auto-generated UUID format');

# argsrepr and kwargsrepr
my $msg7 = create_task_message(
    task_id   => 'repr-1',
    task_name => 'tasks.repr',
    args      => [1, 'hello', undef],
    kwargs    => { key => 'value' },
);
like($msg7->{headers}{argsrepr}, qr/1.*hello.*None/, 'argsrepr looks Pythonic');
like($msg7->{headers}{kwargsrepr}, qr/'key'.*'value'/, 'kwargsrepr looks Pythonic');

done_testing;
