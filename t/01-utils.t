use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Utils qw(first_not_null generate_consumer_tag);

# first_not_null
is(first_not_null(undef, undef, 'a'),   'a',   'first_not_null: skips undefs');
is(first_not_null('b', 'c'),            'b',    'first_not_null: returns first defined');
is(first_not_null(undef, undef),        undef,  'first_not_null: all undef returns undef');
is(first_not_null(),                    undef,  'first_not_null: empty args returns undef');
is(first_not_null(0),                   0,      'first_not_null: 0 is defined');
is(first_not_null(''),                  '',     'first_not_null: empty string is defined');

# generate_consumer_tag
my $tag1 = generate_consumer_tag(prefix => 'test', channel_number => 5);
like($tag1, qr/^testctag5\.[0-9a-f]+$/, 'consumer tag: correct format with prefix');

my $tag2 = generate_consumer_tag(channel_number => 1);
like($tag2, qr/^ctag1\.[0-9a-f]+$/, 'consumer tag: correct format without prefix');

# Uniqueness
my $tag3 = generate_consumer_tag(channel_number => 1);
isnt($tag2, $tag3, 'consumer tag: unique each call');

done_testing;
