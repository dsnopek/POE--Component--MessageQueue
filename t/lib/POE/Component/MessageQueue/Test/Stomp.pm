package POE::Component::MessageQueue::Test::Stomp;
use strict;
use warnings;
use Net::Stomp;
use YAML;
use Exporter qw(import);
our @EXPORT = qw(
	stomp_connect   stomp_send 
	stomp_subscribe stomp_receive
);

sub stomp_connect {
	my $stomp = Net::Stomp->new({
		hostname => 'localhost', 
		port => 8099
	});

	$stomp->connect({
		login    => 'foo', 
		password => 'bar'
	});

	return $stomp;
}

sub make_nonce { 
	my @chars = ['a'..'z', 'A'..'Z'];
	return join('', map { $chars[rand @chars] } (1..20));
}

sub receipt_request {
	my ($stomp, %conf) = @_;
	my $nonce = make_nonce();
	my $frame = Net::Stomp::Frame->new(\%conf);

	$frame->headers->{receipt} = $nonce;
	$stomp->send_frame($frame);
	
	my $receipt = $stomp->receive_frame;

	die "Expected reciept\n" . Dump($receipt) 
		unless ($receipt->command eq 'RECEIPT' 
		&& $receipt->headers->{'receipt-id'} eq $nonce);
}

sub stomp_send {
	receipt_request($_[0],
		command => 'SEND',
		headers => {
			destination => '/queue/test',
			body        => 'arglebargle',
			persistent  => 'true',
		},
	);
}

sub stomp_subscribe {
	receipt_request($_[0],
		command => 'SUBSCRIBE',
		headers => {
			destination => '/queue/test',
			ack         => 'client',
		},
	);
}

sub stomp_receive {
	my $stomp = $_[0];
	my $frame = $stomp->receive_frame();

	receipt_request($stomp, 
		command => 'ACK',
		headers => { 'message-id' => $frame->headers->{'message-id'} },
	);
}

1;
