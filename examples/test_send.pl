
use Net::Stomp;
use Getopt::Long;
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

	$stomp->send({
		destination => "/queue/monkey_bin",
		body        => $data,
		persistent  => 'true',
	});
}

sub main
{
	my $count = 1;

	GetOptions ("count|c=i" => \$count);

	for (my $i = 0; $i < $count; $i++)
	{
		throw_a_monkey;
	}
}
main;

