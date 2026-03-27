use strict;
use warnings;
use Test2::V0;
use JSON::MaybeXS qw(encode_json);
use AIO::Celery::AMQP qw(create_task_message);
use AIO::Celery::Request;

# Round-trip: create_task_message -> Request::from_message
my $msg = create_task_message(
    task_id   => 'round-trip-1',
    task_name => 'tasks.add',
    args      => [10, 20],
    kwargs    => { op => 'sum' },
    retries   => 2,
    parent_id => 'parent-x',
    root_id   => 'root-x',
);

my $req = AIO::Celery::Request->from_message($msg);

is($req->id,       'round-trip-1', 'request id');
is($req->task,     'tasks.add',    'request task');
is($req->args,     [10, 20],       'request args');
is($req->kwargs,   { op => 'sum' }, 'request kwargs');
is($req->retries,  2,               'request retries');
is($req->parent_id, 'parent-x',    'request parent_id');
is($req->root_id,   'root-x',      'request root_id');
is($req->eta,       undef,          'request eta undef when not set');
is($req->ignore_result, 0,          'request ignore_result false');
ref_ok($req->chain, 'ARRAY',        'request chain is array');
is(scalar @{$req->chain}, 0,        'request chain empty');

# With chain
my $msg_chain = create_task_message(
    task_id   => 'chain-1',
    task_name => 'tasks.first',
    args      => [1],
    chain     => [
        { task => 'tasks.second', args => [], kwargs => {}, options => {} },
    ],
);
my $req_chain = AIO::Celery::Request->from_message($msg_chain);
is(scalar @{$req_chain->chain}, 1,              'chain has 1 element');
is($req_chain->chain->[0]{task}, 'tasks.second', 'chain task name');

# With ETA
my $msg_eta = create_task_message(
    task_id   => 'eta-1',
    task_name => 'tasks.delayed',
    countdown => 120,
);
my $req_eta = AIO::Celery::Request->from_message($msg_eta);
ok(defined $req_eta->eta, 'eta parsed');
ok($req_eta->eta->isa('DateTime'), 'eta is DateTime');

# build_retry_message
my $retry_msg = $req->build_retry_message(countdown => 60);
is($retry_msg->{headers}{retries}, 3,              'retry increments retries');
is($retry_msg->{headers}{parent_id}, 'round-trip-1', 'retry sets parent_id');
ok(defined $retry_msg->{headers}{eta},               'retry sets eta');

# Requires countdown
like(
    dies { $req->build_retry_message() },
    qr/countdown is required/,
    'build_retry_message requires countdown',
);

# Missing headers
like(
    dies { AIO::Celery::Request->from_message({}) },
    qr/no headers/,
    'from_message requires headers',
);

like(
    dies { AIO::Celery::Request->from_message({ headers => {} }) },
    qr/no id header/,
    'from_message requires id',
);

like(
    dies { AIO::Celery::Request->from_message({ headers => { id => 'x' } }) },
    qr/no task header/,
    'from_message requires task',
);

done_testing;
