# AIO::Celery

**Async Celery task queue for Perl** — a port of [aio-celery](https://github.com/earlgreyness/aio-celery) from Python to Perl.

Implements the [Celery Message Protocol v2](https://docs.celeryq.dev/en/stable/internals/protocol.html) natively, enabling full interoperability with Python Celery workers and clients.

Built on [IO::Async](https://metacpan.org/pod/IO::Async) + [Future::AsyncAwait](https://metacpan.org/pod/Future::AsyncAwait) for true non-blocking async I/O on a single thread.

## Features

- **RabbitMQ broker** via [Net::Async::AMQP](https://metacpan.org/pod/Net::Async::AMQP)
- **Redis result backend** via [Net::Async::Redis](https://metacpan.org/pod/Net::Async::Redis)
- **Celery protocol compatibility** — publish from Perl, consume in Python (and vice versa)
- **Async worker** with configurable concurrency (up to 10,000+ concurrent tasks)
- **Task chaining** via Canvas signatures
- **Task retry** with configurable backoff and max retries
- **HTTP inspection server** for monitoring running tasks
- **CLI worker** — `aio-celery worker Module::Name:variable`

## Installation

```bash
cpanm AIO::Celery
```

Or from source:

```bash
git clone https://github.com/eekstunt/aio-celery-perl.git
cd aio-celery-perl
cpanm --installdeps .
perl Makefile.PL
make && make test
```

## Quick Start

### Define Tasks

```perl
# myapp.pm
package MyApp;
use AIO::Celery::App;

our $app = AIO::Celery::App->new(name => 'myapp');
$app->conf->update(
    broker_url     => 'amqp://guest:guest@localhost:5672//',
    result_backend => 'redis://localhost:6379/0',
);

$app->task(sub {
    my ($x, $y) = @_;
    return $x + $y;
}, name => 'tasks.add');

$app->task(sub {
    my ($x, $y) = @_;
    return $x * $y;
}, name => 'tasks.multiply');

1;
```

### Run the Worker

```bash
aio-celery worker MyApp:app -l INFO -Q celery
```

### Publish Tasks

```perl
use IO::Async::Loop;
use Future::AsyncAwait;
use AIO::Celery::App;

my $loop = IO::Async::Loop->new;

my $app = AIO::Celery::App->new(name => 'publisher');
$app->conf->update(
    broker_url     => 'amqp://guest:guest@localhost:5672//',
    result_backend => 'redis://localhost:6379/0',
);

$loop->await(async sub {
    await $app->setup(loop => $loop);

    # Publish a single task
    my $result = await $app->send_task('tasks.add', args => [2, 3]);

    # Wait for result
    my $value = await $result->get(timeout => 10);
    print "Result: $value\n";  # 5

    await $app->teardown;
}->());
```

### Batch Publishing (High Throughput)

```perl
$loop->await(async sub {
    await $app->setup(loop => $loop);

    # Publish 50,000 tasks concurrently
    my @futures;
    for my $n (0 .. 49_999) {
        push @futures, $app->send_task('tasks.add', args => [$n, $n]);
    }
    await Future->wait_all(@futures);

    await $app->teardown;
}->());
```

## Python Interoperability

AIO::Celery implements the Celery Message Protocol v2. Tasks published by Perl are consumable by Python Celery workers, and vice versa.

**Perl publisher -> Python worker:**

```perl
# Perl: publish
await $app->send_task('python_tasks.process', args => ['data']);
```

```python
# Python: consume
@app.task
async def process(data):
    return f"processed: {data}"
```

**Python publisher -> Perl worker:**

```python
# Python: publish
await app.send_task("perl_tasks.add", args=(2, 3))
```

```perl
# Perl: consume
$app->task(sub { $_[0] + $_[1] }, name => 'perl_tasks.add');
```

## CLI

```
aio-celery worker Module::Name:variable [OPTIONS]

Options:
  -c, --concurrency=N    Max simultaneous tasks (default: 10000)
  -Q, --queues=LIST      Comma-separated queue names
  -l, --loglevel=LEVEL   DEBUG|INFO|WARNING|ERROR|CRITICAL
  -V, --version          Print version and exit
```

## Configuration

```perl
$app->conf->update(
    broker_url                  => 'amqp://user:pass@rabbit:5672//',
    result_backend              => 'redis://localhost:6379/0',
    task_default_queue          => 'celery',
    task_default_priority       => undef,
    task_queue_max_priority     => 10,
    task_soft_time_limit        => 300,
    task_ignore_result          => 0,
    result_expires              => 86400,
    worker_prefetch_multiplier  => 4,
    inspection_http_server_enabled => 1,
    inspection_http_server_port    => 1430,
);
```

## Benchmarks

Compared against the original Python [aio-celery](https://github.com/earlgreyness/aio-celery) v0.22.0.

**50,000 tasks, 5 runs averaged, RabbitMQ in Docker:**

| Benchmark | Python aio-celery | Perl AIO::Celery | Ratio |
|-----------|------------------|-----------------|-------|
| Serialize messages | 148k msg/s | **160k msg/s** | 1.1x |
| Deserialize messages | **772k msg/s** | 87k msg/s | 0.1x |
| Publish to RabbitMQ | 3.2k msg/s | 63k msg/s | 19.5x |

See `bench/` for reproducible benchmark scripts.

## Architecture

All modules are in the `AIO::Celery::` namespace:

| Module | Purpose |
|--------|---------|
| `App` | Central hub: task registration, connections, send_task |
| `Worker` | Async consumption loop, concurrency control, execution |
| `Task` | Execution metadata, state updates, retry, chain forwarding |
| `AMQP` | Celery Message Protocol v2 serialization |
| `Broker` | RabbitMQ publishing via Net::Async::AMQP |
| `Backend` | Redis connection via Net::Async::Redis |
| `Config` | Configuration with validation |
| `Result` | AsyncResult — poll Redis for task results |
| `Canvas` | Signature and chain() for task composition |
| `AnnotatedTask` | Task decorator wrapper (coderef -> task system) |
| `Request` | AMQP message deserialization |
| `Exceptions` | CeleryError, Retry, Timeout, MaxRetriesExceeded |
| `InspectionHTTPServer` | HTTP /health endpoint for monitoring |

## Requirements

- Perl 5.26+
- RabbitMQ (broker)
- Redis (optional, for result backend)

## Acknowledgments

Inspired by and ported from [aio-celery](https://github.com/earlgreyness/aio-celery) by [earlgreyness](https://github.com/earlgreyness) (Kirill). The original Python library demonstrated that a clean, minimal Celery reimplementation is both possible and practical.

## License

MIT License. See [LICENSE](LICENSE) for details.
