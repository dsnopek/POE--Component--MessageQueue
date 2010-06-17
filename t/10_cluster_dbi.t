use strict;
use warnings;

use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use POE::Component::MessageQueue::Storage::DBI;
use File::Path;
use Test::More;
use Test::Exception;
use DBI;

# Make sure that there are no conflicts between in_use_by values when two
# seperate MQs are using Storage::DBI pointing at the same database.

# This test requires an external (not SQLite) database to work.  The user must setup
# this database in advance of the test or it will be skipped.

BEGIN {
	if (!defined $ENV{'POCOMQ_TEST_DSN'}) {
		plan skip_all => "This test requires an external database (with correct tables already defined).  Set the following environment variables to cause the test to run: POCOMQ_TEST_DSN, POCOMQ_TEST_USERNAME, POCOMQ_TEST_PASSWORD";
		exit 0;
	}
}

my $dsn = $ENV{'POCOMQ_TEST_DSN'};
my $username = $ENV{'POCOMQ_TEST_USERNAME'};
my $password = $ENV{'POCOMQ_TEST_PASSWORD'};

# clean database
DBI->connect($dsn, $username, $password)
   ->do("DELETE FROM messages");

sub storage_factory {
	my %args1 = @_;
	my $storage = sub {
		my %args2 = @_;
		return POE::Component::MessageQueue::Storage::DBI->new(
			dsn      => $dsn,
			username => $username,
			password => $password,
			%args1,
			%args2,
		);
	};
}

plan tests => 11; 

my $pid1 = start_mq(
	storage => storage_factory(mq_id => 'mq1'),
);
sleep 2;
ok($pid1, "MQ1 started");

my $pid2 = start_mq(
	storage => storage_factory(mq_id => 'mq2'),
	port => '8100',
);
sleep 2;
ok($pid2, "MQ2 started");

# Test #1
#
# This test works by checking if one MQ will incorrectly clear the claims set by
# the other MQ.
#
# So, first we have one client (id 1) connect to MQ1, subscribe and then send a
# message.  This will get it claimed immediately, but we won't ACK.
#
# Next, connect and disconnect to MQ2 (will also have id 1).  This will cause MQ2
# to clear all claims made by this client.
#
# Hopefully, MQ2 clearing client 1 won't cause the claim for MQ1's client 1 to be
# cleared.
#
# We test that this has worked by having another client connect to MQ1 and subscribe
# and make sure that the message isn't redelivered.
#

my ($client1a, $client1b, $client2);
my $message;

ok($client1a = stomp_connect(), 'MQ1: client 1 connected');

lives_ok {
	stomp_subscribe($client1a);
	stomp_send($client1a);
} 'MQ1: client 1 subscribed and sent message';

ok($message = $client1a->receive_frame(), 'MQ1: client 1 claimed message');
sleep 2;

# next, client 1 connects and disconnects to MQ2
ok($client1b = stomp_connect(8100),  'MQ2: client 1 connects');
lives_ok { $client1b->disconnect() } 'MQ2: client 1 disconnects';

# finally, client 2 connects to MQ1, subscribes and makes sure that 
# this first message isn't delivered
lives_ok {
	$client2 = stomp_connect();
	stomp_subscribe($client2);
} 'MQ1: client 2 subscribes';
is($client2->can_read({ timeout => 10 }), 0, 'message isn\'t re-delivered');

ok(stop_fork($pid1), 'MQ1 shut down.');
ok(stop_fork($pid2), 'MQ2 shut down.');

