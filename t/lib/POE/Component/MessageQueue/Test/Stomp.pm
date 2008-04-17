package POE::Component::MessageQueue::Test::Stomp;
use strict;
use warnings;
use Net::Stomp;
use Exporter qw(import);
our @EXPORT = qw(
	stomp_connect   stomp_send 
	stomp_subscribe stomp_receive
);

sub stomp_connect {
	my $stomp = Net::Stomp->new({hostname => 'localhost', port => 8099});
	$stomp->connect({login => 'foo', password => 'bar'});
	return $stomp;
}

sub stomp_send {
	my $stomp = $_[0];
	$stomp->send({
		destination => '/queue/test',
		body => 'arglebargle',
		persistent => 1,
	});
}

sub stomp_subscribe {
	my $stomp = $_[0];
	$stomp->subscribe({
		destination => '/queue/test',
		ack => 'client',
	});
}

sub stomp_receive {
	my $stomp = $_[0];
	my $frame = $stomp->receive_frame();
	$stomp->ack({frame => $frame});
}

1;
