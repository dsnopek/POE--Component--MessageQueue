use strict;
use warnings;

use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use Test::More tests => 8;
use Test::Exception;

# Once upon a time, we had a bug where the MQ would crash if you connected,
# sent some messages, received them, disconnected, reconnected, and sent 
# some more.

my $pid = start_mq(); sleep 2;
ok($pid, "MQ started");

foreach my $i (1..2) {
	my $receiver;
	lives_ok { 
		$receiver = stomp_connect();
		stomp_subscribe($receiver);
	} "Subscribed: $i";

	lives_ok {
		my $sender = stomp_connect();
		stomp_send($sender) for (1..10);
		$sender->disconnect;
	} "Sent:       $i";

	lives_ok {
		stomp_receive($receiver) for (1..10);
		$receiver->disconnect;
	} "Received:   $i";
}

ok(stop_mq($pid), 'MQ shut down.');
