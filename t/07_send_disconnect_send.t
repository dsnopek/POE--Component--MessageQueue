use strict;
use warnings;

use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use Test::More tests => 1;

# Once upon a time, we had a bug where the MQ would crash if you connected,
# sent some messages, received them, disconnected, reconnected, and sent 
# some more.

my $pid = start_mq(); sleep 2;

for (1..2) {
	my $receiver = stomp_connect();
	stomp_subscribe($receiver);

	my $sender = stomp_connect();
	stomp_send($sender) for (1..10);
	$sender->disconnect;

	stomp_receive($receiver);
	$receiver->disconnect;
}

ok(stop_mq($pid), 'MQ shut down.');
