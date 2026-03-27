use strict;
use warnings;
use Test2::V0;

my @modules = qw(
    AIO::Celery
    AIO::Celery::App
    AIO::Celery::AMQP
    AIO::Celery::AnnotatedTask
    AIO::Celery::Backend
    AIO::Celery::Broker
    AIO::Celery::Canvas
    AIO::Celery::Config
    AIO::Celery::Context
    AIO::Celery::Exceptions
    AIO::Celery::InspectionHTTPServer
    AIO::Celery::IntermittentGC
    AIO::Celery::Request
    AIO::Celery::Result
    AIO::Celery::State
    AIO::Celery::Task
    AIO::Celery::Utils
    AIO::Celery::Worker
);

for my $mod (@modules) {
    ok(eval "require $mod; 1", "load $mod") or diag("Failed to load $mod: $@");
}

done_testing;
