use strict;
use warnings;
use Test2::V0;
use JSON::MaybeXS qw(decode_json encode_json);
use AIO::Celery::AMQP qw(create_task_message);
use AIO::Celery::Request;

# Test that messages created by AIO::Celery are compatible with
# the Celery Message Protocol v2 as consumed by Python Celery.

# --- Protocol v2 structure validation ---
my $msg = create_task_message(
    task_id   => 'proto-test-001',
    task_name => 'myapp.tasks.process',
    args      => [42, 'hello', undef],
    kwargs    => { timeout => 30, verbose => 1 },
    priority  => 7,
    parent_id => 'parent-abc',
    root_id   => 'root-xyz',
    retries   => 2,
    chain     => [
        { task => 'myapp.tasks.finalize', args => [], kwargs => {}, options => {} },
    ],
    ignore_result => 1,
    countdown     => 60,
);

# Verify top-level message structure
my @required_keys = qw(body headers content_type content_encoding delivery_mode correlation_id reply_to);
for my $key (@required_keys) {
    ok(exists $msg->{$key}, "message has '$key'");
}

# Verify content negotiation
is($msg->{content_type}, 'application/json', 'content_type is JSON');
is($msg->{content_encoding}, 'utf-8', 'content_encoding is UTF-8');
is($msg->{delivery_mode}, 2, 'delivery_mode is persistent');
is($msg->{priority}, 7, 'priority preserved');
is($msg->{correlation_id}, 'proto-test-001', 'correlation_id matches task_id');

# Verify headers (Celery protocol v2)
my $h = $msg->{headers};
is($h->{id}, 'proto-test-001', 'header id');
is($h->{task}, 'myapp.tasks.process', 'header task');
is($h->{lang}, 'py', 'lang must be py for Python compat');
is($h->{retries}, 2, 'retries preserved');
is($h->{root_id}, 'root-xyz', 'root_id');
is($h->{parent_id}, 'parent-abc', 'parent_id');
ok(defined $h->{eta}, 'eta set from countdown');
ok(JSON::MaybeXS::is_bool($h->{ignore_result}), 'ignore_result is bool');
ok($h->{ignore_result}, 'ignore_result is true');
ref_ok($h->{timelimit}, 'ARRAY', 'timelimit is array');
is(scalar @{$h->{timelimit}}, 2, 'timelimit has 2 elements');
ref_ok($h->{stamps}, 'HASH', 'stamps is hash');
like($h->{origin}, qr/\d+\@/, 'origin has pid@hostname');

# Verify body structure: [args, kwargs, {callbacks, errbacks, chain, chord}]
my $body = decode_json($msg->{body});
ref_ok($body, 'ARRAY', 'body is array');
is(scalar @$body, 3, 'body has 3 elements');

my ($b_args, $b_kwargs, $b_options) = @$body;
is($b_args, [42, 'hello', undef], 'body args match');
is($b_kwargs, { timeout => 30, verbose => 1 }, 'body kwargs match');
is($b_options->{callbacks}, undef, 'callbacks is null');
is($b_options->{errbacks}, undef, 'errbacks is null');
is($b_options->{chord}, undef, 'chord is null');
ref_ok($b_options->{chain}, 'ARRAY', 'chain is array');
is(scalar @{$b_options->{chain}}, 1, 'chain has 1 element');
is($b_options->{chain}[0]{task}, 'myapp.tasks.finalize', 'chain task name');

# --- Round-trip: serialize -> deserialize ---
my $req = AIO::Celery::Request->from_message($msg);
is($req->id, 'proto-test-001', 'round-trip id');
is($req->task, 'myapp.tasks.process', 'round-trip task');
is($req->args, [42, 'hello', undef], 'round-trip args');
is($req->kwargs, { timeout => 30, verbose => 1 }, 'round-trip kwargs');
is($req->retries, 2, 'round-trip retries');
is($req->parent_id, 'parent-abc', 'round-trip parent_id');
is($req->root_id, 'root-xyz', 'round-trip root_id');
ok($req->ignore_result, 'round-trip ignore_result');
ok(defined $req->eta, 'round-trip eta parsed');
is(scalar @{$req->chain}, 1, 'round-trip chain length');

# --- Simulate Python-originated message ---
# This is what a Python Celery/aio-celery publisher would produce
my $python_msg = {
    body => encode_json([
        [1, 2],
        {},
        { callbacks => undef, errbacks => undef, chain => [], chord => undef },
    ]),
    headers => {
        id              => 'py-task-abc123',
        task            => 'tasks.add',
        lang            => 'py',
        retries         => 0,
        root_id         => 'py-task-abc123',
        parent_id       => undef,
        group           => undef,
        eta             => undef,
        expires         => undef,
        timelimit       => [undef, undef],
        ignore_result   => JSON::MaybeXS::false,
        argsrepr        => '(1, 2)',
        kwargsrepr      => '{}',
        origin          => '12345@producer-host',
        shadow          => undef,
        stamped_headers => undef,
        stamps          => {},
    },
    content_type     => 'application/json',
    content_encoding => 'utf-8',
    delivery_mode    => 2,
};

my $py_req = AIO::Celery::Request->from_message($python_msg);
is($py_req->id, 'py-task-abc123', 'Python msg: id');
is($py_req->task, 'tasks.add', 'Python msg: task');
is($py_req->args, [1, 2], 'Python msg: args');
is($py_req->kwargs, {}, 'Python msg: kwargs');
is($py_req->retries, 0, 'Python msg: retries');
is($py_req->eta, undef, 'Python msg: no eta');
ok(!$py_req->ignore_result, 'Python msg: ignore_result false');

done_testing;
