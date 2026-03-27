use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Worker;

# Basic semaphore
my $sem = AIO::Celery::Worker::Semaphore->new(max_concurrent => 3);
is($sem->active_count, 0, 'starts at 0');

# Acquire within limit returns done future
my $f1 = $sem->acquire;
ok($f1->is_done, 'first acquire immediate');
is($sem->active_count, 1, 'count 1');

my $f2 = $sem->acquire;
ok($f2->is_done, 'second acquire immediate');
is($sem->active_count, 2, 'count 2');

my $f3 = $sem->acquire;
ok($f3->is_done, 'third acquire immediate');
is($sem->active_count, 3, 'count 3');

# Fourth acquire should block (return pending future)
my $f4 = $sem->acquire;
ok(!$f4->is_done, 'fourth acquire blocks');

# Release one — should unblock f4
$sem->release;
ok($f4->is_done, 'fourth unblocked after release');

# Multiple waiters
my $f5 = $sem->acquire;
ok(!$f5->is_done, 'fifth blocks');
my $f6 = $sem->acquire;
ok(!$f6->is_done, 'sixth blocks');

$sem->release;
ok($f5->is_done, 'fifth unblocked (FIFO)');
ok(!$f6->is_done, 'sixth still blocked');

$sem->release;
ok($f6->is_done, 'sixth unblocked');

# Release all
$sem->release;
$sem->release;
$sem->release;
is($sem->active_count, 0, 'back to 0 after all releases');

done_testing;
