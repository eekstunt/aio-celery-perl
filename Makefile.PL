use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'AIO::Celery',
    AUTHOR           => 'AIO-Celery Contributors',
    VERSION_FROM     => 'lib/AIO/Celery.pm',
    ABSTRACT         => 'Async Celery task queue implementation for Perl',
    LICENSE          => 'mit',
    MIN_PERL_VERSION => '5.026',
    EXE_FILES        => ['bin/aio-celery'],
    PREREQ_PM        => {
        'IO::Async'                   => '0.80',
        'Future'                      => '0.49',
        'Future::AsyncAwait'          => '0.66',
        'Net::Async::AMQP'           => '2.000',
        'Net::Async::Redis'          => '3.000',
        'JSON::MaybeXS'              => '1.004',
        'Data::UUID'                 => '0',
        'Moo'                        => '2.005',
        'Types::Standard'            => '0',
        'namespace::clean'           => '0',
        'Try::Tiny'                  => '0.31',
        'Throwable'                  => '0',
        'Net::Async::HTTP::Server'   => '0.13',
        'DateTime'                   => '0',
        'DateTime::Format::ISO8601'  => '0',
        'URI'                        => '0',
        'Log::Any'                   => '0',
    },
    TEST_REQUIRES => {
        'Test2::V0'        => '0',
        'Test::MockModule' => '0',
        'Test::MockObject' => '0',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
        resources   => {
            repository => {
                type => 'git',
                url  => 'https://github.com/eekstunt/aio-celery-perl.git',
                web  => 'https://github.com/eekstunt/aio-celery-perl',
            },
        },
    },
);
