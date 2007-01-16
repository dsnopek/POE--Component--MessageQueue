
use Net::Stomp;
use strict;

my $MAX_THREADS  = 100;
my $MONKEY_COUNT = 10000;
my $USERNAME     = 'system';
my $PASSWORD     = 'manager';

my $stomp = Net::Stomp->new({
	hostname => 'localhost',
	port     => 61613
});
$stomp->connect({ login => $USERNAME, passcode => $PASSWORD });
$stomp->subscribe({
	'destination'           => '/queue/monkey_bin',
	'ack'                   => 'client',
	'activemq.prefetchSize' => 1 
});
while (1)
{
	my $frame = $stomp->receive_frame;
	print $frame->body . "\n";
	$stomp->ack({ frame => $frame });
}
$stomp->disconnect();

