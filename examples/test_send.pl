
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

sub throw_a_monkey
{
	my $data = "monkey";

	#my $stomp = Net::Stomp->new({
	#	hostname => "$LM_MESSAGE_QUEUE_HOST",
	#	port     => 61613
	#});
	#$stomp->connect({ login => $USERNAME, passcode => $PASSWORD });
	$stomp->send({
		destination => "/queue/monkey_bin",
		body        => $data,
		persistent  => 'true',
	});
	#$stomp->disconnect();
}

throw_a_monkey;

