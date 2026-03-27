requires 'perl', '5.026';

# Async framework
requires 'IO::Async', '0.80';
requires 'Future', '0.49';
requires 'Future::AsyncAwait', '0.66';

# AMQP
requires 'Net::Async::AMQP', '2.000';

# Redis
requires 'Net::Async::Redis', '3.000';

# JSON
requires 'JSON::MaybeXS', '1.004';

# UUID
requires 'Data::UUID';

# OO framework
requires 'Moo', '2.005';
requires 'Types::Standard';
requires 'namespace::clean';

# Exception handling
requires 'Try::Tiny', '0.31';
requires 'Throwable';

# HTTP server (inspection endpoint)
requires 'Net::Async::HTTP::Server', '0.13';

# Date/Time
requires 'DateTime';
requires 'DateTime::Format::ISO8601';

# URI parsing
requires 'URI';

# Logging
requires 'Log::Any';

on 'test' => sub {
    requires 'Test2::V0';
    requires 'Test::MockModule';
    requires 'Test::MockObject';
};
