use strict;
use warnings;
use POE;
use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;

# Once upon a time, we had a bug where the MQ would crash if you connected,
# sent some messages, received them, disconnected, reconnected, and sent 
# some more.

if(my $pid = fork) {
	sleep 2;

	for (1..2) {
		my $receiver = stomp_connect();
		stomp_subscribe($receiver);

		my $sender = stomp_connect();
		stomp_send($sender) for (1..10);
		$sender->disconnect;

		stomp_receive($receiver);
		$receiver->disconnect;
	}
	kill "TERM", $pid;
}
else {
	use Test::Exception;
	use Test::More;
	plan tests => 1;
	use POE::Component::MessageQueue;
	use POE::Component::MessageQueue::Storage::Memory;
	use POE::Component::MessageQueue::Logger;
	POE::Component::MessageQueue->new(
		port    => 8099, # test will fail if this port isn't open
		storage => POE::Component::MessageQueue::Storage::Memory->new,
		logger  => POE::Component::MessageQueue::Logger->new(level=>7),
	);

	lives_ok {
		$poe_kernel->run();
	} 'send, recv, disconnect, send, recv worked fine';
}

