use strict;
use warnings;
use Test2::V0;
use AIO::Celery::State qw(get_current_app set_current_app %RUNNING_TASKS $TASKS_SINCE_LAST_GC);

# No current app initially
$AIO::Celery::State::CURRENT_APP = undef;
like(
    dies { get_current_app() },
    qr/No current AIO::Celery app/,
    'get_current_app dies when unset',
);

# Set and get
my $fake_app = bless {}, 'FakeApp';
set_current_app($fake_app);
is(get_current_app(), $fake_app, 'set/get current app');

# RunningTask
my $rt = AIO::Celery::State::RunningTask->new(
    task_id   => 'task-1',
    task_name => 'tasks.add',
    args      => [1, 2],
    retries   => 0,
);
is($rt->task_id,   'task-1',    'running task id');
is($rt->task_name, 'tasks.add', 'running task name');
is($rt->state,     'RUNNING',   'running task default state');
ok($rt->elapsed >= 0,           'elapsed >= 0');

# serialize
my $s = $rt->serialize;
is($s->{id},   'task-1',    'serialized id');
is($s->{name}, 'tasks.add', 'serialized name');
is($s->{state}, 'RUNNING',  'serialized state');
ok(exists $s->{elapsed},    'serialized has elapsed');
ok(exists $s->{started},    'serialized has started');

# State change
$rt->state('SLEEPING');
is($rt->state, 'SLEEPING', 'state change');

# RUNNING_TASKS
$RUNNING_TASKS{'task-1'} = $rt;
is(scalar keys %RUNNING_TASKS, 1, 'RUNNING_TASKS has entry');
delete $RUNNING_TASKS{'task-1'};
is(scalar keys %RUNNING_TASKS, 0, 'RUNNING_TASKS cleaned');

# GC counter
$TASKS_SINCE_LAST_GC = 0;
$TASKS_SINCE_LAST_GC++;
is($TASKS_SINCE_LAST_GC, 1, 'GC counter increments');

# Cleanup
$AIO::Celery::State::CURRENT_APP = undef;

done_testing;
