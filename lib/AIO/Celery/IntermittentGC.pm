package AIO::Celery::IntermittentGC;
use strict;
use warnings;
use Log::Any qw($log);
use AIO::Celery::State qw(%RUNNING_TASKS $TASKS_SINCE_LAST_GC);

# Perl uses reference counting + cycle collector.
# This module provides the same interface as the Python version
# but its actual GC behavior is minimal — Perl handles GC automatically.
# The "pause" mechanism is preserved for potential memory pressure scenarios.

my $_last_gc_time;
my $_running = 0;

sub start {
    my (%args) = @_;
    my $loop      = $args{loop}      // die "loop is required";
    my $config    = $args{config}    // die "config is required";
    my $interval  = $config->intermittent_gc_max_interval;
    my $threshold = $config->intermittent_gc_max_tasks;

    return unless $config->intermittent_gc_enabled;

    $_last_gc_time = time();
    $_running = 1;

    $log->info("IntermittentGC started (interval=${interval}s, threshold=${threshold} tasks)");

    $loop->watch_time(
        after    => 0.5,
        interval => 0.5,
        code     => sub {
            return unless $_running;
            _check_and_collect($interval, $threshold);
        },
    );
}

sub stop {
    $_running = 0;
}

sub _check_and_collect {
    my ($interval, $threshold) = @_;

    my $elapsed = time() - $_last_gc_time;
    my $should_collect = 0;

    if ($TASKS_SINCE_LAST_GC >= $threshold) {
        $log->debug("GC triggered: task count threshold ($TASKS_SINCE_LAST_GC >= $threshold)");
        $should_collect = 1;
    }
    if ($elapsed >= $interval) {
        $log->debug("GC triggered: time interval (${elapsed}s >= ${interval}s)");
        $should_collect = 1;
    }

    return unless $should_collect;

    # Wait for running tasks to complete
    if (keys %RUNNING_TASKS) {
        $log->debug("GC deferred: " . scalar(keys %RUNNING_TASKS) . " tasks still running");
        return;
    }

    $log->debug("Running garbage collection");
    $_last_gc_time = time();
    $TASKS_SINCE_LAST_GC = 0;

    # In Perl, we don't have explicit GC to trigger like Python's gc.collect().
    # We can undef large structures if needed, but normally Perl's
    # reference counting handles this automatically.
}

1;
