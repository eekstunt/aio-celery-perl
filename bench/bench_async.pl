#!/usr/bin/env perl
#
# Benchmark: 10,000 async HTTP tasks on a single thread.
#
# Proves that AIO::Celery's IO::Async event loop handles massive
# concurrent I/O without threads or forks.
#
# Concurrency is capped at 500 in-flight requests (TCP socket limit),
# but all 10,000 tasks are queued and dispatched asynchronously.
#
use strict;
use warnings;
use lib 'lib';
use Time::HiRes qw(time);
use IO::Async::Loop;
use Net::Async::HTTP;
use Future;
use Future::AsyncAwait;
use URI;

# --- Our Semaphore from AIO::Celery::Worker ---
use AIO::Celery::Worker;

my $NUM_TASKS       = $ARGV[0] // 10_000;
my $MAX_IN_FLIGHT   = $ARGV[1] // 500;
my $NGINX_URL       = $ENV{NGINX_URL} // 'http://127.0.0.1:8888/';

print "=" x 60, "\n";
print "AIO::Celery async benchmark\n";
print "=" x 60, "\n";
printf "Tasks:           %d\n", $NUM_TASKS;
printf "Max in-flight:   %d (concurrent TCP connections)\n", $MAX_IN_FLIGHT;
printf "Target:          %s\n", $NGINX_URL;
printf "PID:             %d (single process, single thread)\n", $$;
printf "Perl:            %s\n", $^V;
print "=" x 60, "\n\n";

my $loop = IO::Async::Loop->new;

my $http = Net::Async::HTTP->new(
    max_connections_per_host => $MAX_IN_FLIGHT,
    timeout                  => 30,
    stall_timeout            => 30,
);
$loop->add($http);

my $semaphore = AIO::Celery::Worker::Semaphore->new(
    max_concurrent => $MAX_IN_FLIGHT,
);

my $completed  = 0;
my $failed     = 0;
my $in_flight  = 0;
my $max_seen   = 0;
my $start_time = time();

# Track concurrent requests per second
my %second_counts;

# Progress reporter
my $progress_timer = $loop->watch_time(
    after    => 0.5,
    interval => 0.5,
    code     => sub {
        my $elapsed = time() - $start_time;
        my $rps = $completed / ($elapsed || 1);
        printf "\r  [%.1fs] %d/%d done, %d in-flight, %d failed — %.0f req/s   ",
            $elapsed, $completed, $NUM_TASKS, $in_flight, $failed, $rps;
    },
);

async sub run_task {
    my ($task_id) = @_;

    await $semaphore->acquire;
    $in_flight++;
    $max_seen = $in_flight if $in_flight > $max_seen;

    my $second_key = int(time());

    my ($success, $resp);
    eval {
        $resp = await $http->do_request(
            uri => URI->new($NGINX_URL . "?task=$task_id"),
        );
        $success = 1;
    };

    $in_flight--;
    $semaphore->release;

    if ($success && $resp && $resp->is_success) {
        $completed++;
        $second_counts{$second_key}++;
    } else {
        $failed++;
    }
}

print "Launching $NUM_TASKS tasks...\n";
my $launch_start = time();

my @futures;
for my $i (1 .. $NUM_TASKS) {
    push @futures, run_task($i);
}

my $launch_time = time() - $launch_start;
printf "All %d futures created in %.3fs\n", $NUM_TASKS, $launch_time;
print "Waiting for completion...\n";

$loop->await(Future->wait_all(@futures));

$loop->unwatch_time($progress_timer);

my $total_time = time() - $start_time;

# Final report
print "\n\n";
print "=" x 60, "\n";
print "RESULTS\n";
print "=" x 60, "\n";
printf "Total tasks:     %d\n",    $NUM_TASKS;
printf "Completed:       %d\n",    $completed;
printf "Failed:          %d\n",    $failed;
printf "Total time:      %.3fs\n", $total_time;
printf "Throughput:      %.0f req/s\n", $completed / ($total_time || 1);
printf "Avg latency:     %.3fms\n", ($total_time / ($NUM_TASKS || 1)) * 1000;
printf "Max in-flight:   %d (observed peak)\n", $max_seen;
printf "Process:         single thread, PID %d\n", $$;
print "=" x 60, "\n";

# Per-second breakdown
if (%second_counts) {
    print "\nRequests completed per second (client-side):\n";
    my @sorted = sort { $a <=> $b } keys %second_counts;
    my $base = $sorted[0];
    for my $sec (@sorted) {
        printf "  t+%ds: %d requests\n", $sec - $base, $second_counts{$sec};
    }
}

# Verdict
print "\n";
if ($completed == $NUM_TASKS) {
    print "VERDICT: ALL $NUM_TASKS tasks completed on a single thread.\n";
    print "Peak concurrency: $max_seen simultaneous requests.\n";
    print "Async I/O confirmed.\n";
} elsif ($completed > 0) {
    print "VERDICT: $completed/$NUM_TASKS completed. $failed failed.\n";
} else {
    print "VERDICT: All tasks failed. Is Nginx running on $NGINX_URL?\n";
}
print "\n";

exit($failed > 0 ? 1 : 0);
