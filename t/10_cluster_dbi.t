use strict;
use warnings;

use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use POE::Component::MessageQueue::Test::EngineMaker;
use File::Path;
use Test::More tests => 4;
use Test::Exception;

# Make sure that there are no conflicts between in_use_by values when two
# seperate MQs are using Storage::DBI pointing at the same database.

END {
	rmtree(DATA_DIR);	
}

rmtree(DATA_DIR);
mkpath(DATA_DIR);
make_db();

my $pid1 = start_mq(
	storage => 'DBI',
	storage_args => { mq_id => 'mq1' },
);
sleep 2;
ok($pid1, "MQ1 started");

my $pid2 = start_mq(
	storage => 'DBI',
	storage_args => { mq_id => 'mq2' },
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

my ($client1a, $client1b);
my $message;

ok($client1a = stomp_connect(), 'MQ1: client 1 connected');

lives_ok {
	stomp_subscribe($client1a);
	stomp_send($client1a);
} 'MQ1: client 1 subscribed and sent message';

ok($message = $client1a->receive_frame(), 'MQ1: client 1 claimed message');

# next, client 1 connects and disconnects to MQ2
ok($client1b = stomp_connect(8100),  'MQ2: client 1 connects');
lives_ok { $client1b->disconnect() } 'MQ2: client 1 disconnects';

# finally, client 2 connects and subscribes to MQ1 and makes sure that 
# this first message isn't delivered
is(stomp_connect()->can_read({ timeout => 10 }), 0, 'message isn\'t re-delivered');

ok(stop_fork($pid1), 'MQ1 shut down.');
ok(stop_fork($pid2), 'MQ2 shut down.');

