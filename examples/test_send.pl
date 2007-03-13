
use Net::Stomp;
use Getopt::Long;
use Data::Random qw/rand_image/;
use MIME::Base64;
use strict;

my $MAX_THREADS  = 100;
my $MONKEY_COUNT = 10000;
my $USERNAME     = 'system';
my $PASSWORD     = 'manager';

sub throw_a_monkey
{
	my $stomp = shift;
	my $data  = "monkey";

	$stomp->send({
		destination => "/queue/monkey_bin",
		body        => $data,
		persistent  => 'true',
	});
}

sub throw_an_image
{
	my $stomp = shift;
	my $data  = rand_image(width => 640, height => 480);

	$stomp->send({
		destination => "/queue/monkey_bin",
		body        => encode_base64( $data ),
		persistent  => 'true',
	});
}

sub main
{
	my $count = 1;
	my $image = 0;
	my $fork  = 0;

	GetOptions(
		"count|c=i" => \$count,
		"fork|f=i"  => \$fork,
		'image|i'   => \$image
	);

	while ( $fork-- > 1 )
	{
		# for child and drop out of loop
		fork() or last;
	}

	my $stomp = Net::Stomp->new({
		hostname => 'localhost',
		port     => 61613
	});
	$stomp->connect({ login => $USERNAME, passcode => $PASSWORD });

	for (my $i = 0; $i < $count; $i++)
	{
		if ( $image )
		{
			throw_an_image($stomp);
		}
		else
		{
			throw_a_monkey($stomp);
		}
	}
}
main;

