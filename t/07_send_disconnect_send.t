use strict;
use warnings;
use POE;

# Once upon a time, we had a bug where the MQ would crash if you connected,
# sent some messages, received them, disconnected, reconnected, and sent 
# some more.

sub stomp_connect {
	my $stomp = Net::Stomp->new({hostname => 'localhost', port => 8099});
	$stomp->connect({login => 'foo', password => 'bar'});
	return $stomp;
}

sub send_stuff {
	my $stomp = stomp_connect();
	for (1..10) {
		$stomp->send({
			destination => '/queue/test7',
			body => 'arglebargle',
			persistent => 1,
		});
	}
	$stomp->disconnect;
}

if(my $pid = fork) {
	use Net::Stomp;
	sleep 2;

	for (1..2) {
		my $stomp = stomp_connect();
		$stomp->subscribe({
			destination => '/queue/test7',
			ack => 'client',
		});

		send_stuff();

		for (1..10) {
			my $frame = $stomp->receive_frame();
			$stomp->ack({frame => $frame});
		}
		$stomp->disconnect;
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

