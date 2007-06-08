#!/usr/bin/perl

use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Complex;
use Getopt::Long;
use Carp;
use strict;

$SIG{__DIE__} = sub {
    Carp::confess(@_);
};

my $DATA_DIR = '/var/lib/perl_mq';
my $CONF_DIR = '/etc/perl_mq';
my $CONF_LOG = "$CONF_DIR/log.conf";

my $port     = 61613;
my $hostname = undef;
my $timeout  = 4;
my $throttle_max = 2;

GetOptions(
	"port|p=i"     => \$port,
	"hostname|h=s" => \$hostname,
	"timeout|i=i"  => \$timeout,
	"throttle|T=i" => \$throttle_max,
	"data-dir=s"   => \$DATA_DIR,
	"log-conf=s"   => \$CONF_LOG
);

if ( not -d $DATA_DIR )
{
	mkdir $DATA_DIR;

	if ( not -d $DATA_DIR )
	{
		die "Unable to create the data dir: $DATA_DIR";
	}
}
else
{
	print "Huh?!\n";
}

my $logger_alias;
if ( -e $CONF_LOG )
{
	$logger_alias = 'mq_logger';

	# we create a logger, because a production message queue would
	# really need one.
	POE::Component::Logger->spawn(
		ConfigFile => $CONF_LOG,
		Alias      => $logger_alias
	);
}
else
{
	print STDERR "LOGGER: Unable to find configuration: $CONF_LOG\n";
	print STDERR "LOGGER: Will send all messages to STDERR\n";
}

POE::Component::MessageQueue->new({
	port     => $port,
	hostname => $hostname,

	storage => POE::Component::MessageQueue::Storage::Complex->new({
		data_dir     => $DATA_DIR,
		timeout      => $timeout,
		throttle_max => $throttle_max
	}),

	logger_alias => $logger_alias,
});

POE::Kernel->run();
exit;

