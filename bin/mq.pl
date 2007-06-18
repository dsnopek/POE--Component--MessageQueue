#!/usr/bin/perl

use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Complex;
use Getopt::Long;
use Devel::StackTrace;
use IO::File;
use Carp;
use POSIX qw(setsid strftime);
use strict;

my $DATA_DIR = '/var/lib/perl_mq';
my $CONF_DIR = '/etc/perl_mq';
my $CONF_LOG = "$CONF_DIR/log.conf";

$SIG{__DIE__} = sub {
	# keep track of message queue crashes for later debugging
	my $trace = Devel::StackTrace->new;
	my $fd = IO::File->new(">>$DATA_DIR/crashed.log");
	my (@l) = localtime(time());
	$fd->write("\n============================== \n");
	$fd->write(" Crashed: ".strftime('%Y-%m-%d %H:%M:%S', @l));
	$fd->write("\n============================== \n\n");
	$fd->write( $trace->as_string );
	$fd->close();

	# spit out a stack trace
    Carp::confess(@_);
};

my $port     = 61613;
my $hostname = undef;
my $timeout  = 4;
my $throttle_max = 2;
my $background = 0;
my $pidfile;

GetOptions(
	"port|p=i"     => \$port,
	"hostname|h=s" => \$hostname,
	"timeout|i=i"  => \$timeout,
	"throttle|T=i" => \$throttle_max,
	"data-dir=s"   => \$DATA_DIR,
	"log-conf=s"   => \$CONF_LOG,
	"background|b" => \$background,
	"pidfile|p=s" => \$pidfile,
);

if ( not -d $DATA_DIR )
{
	mkdir $DATA_DIR;

	if ( not -d $DATA_DIR )
	{
		die "Unable to create the data dir: $DATA_DIR";
	}
}

if ( $background )
{   
	# the simplest daemonize, ever.
	defined(fork() && exit 0) or "Can't fork: $!";
	setsid or die "Can't start a new session: $!";
	open STDIN,  '/dev/null' or die "Can't redirect STDIN from /dev/null: $!";
	open STDOUT, '>/dev/null' or die "Can't redirect STDOUT to /dev/null: $!";
	open STDERR, '>/dev/null' or die "Can't redirect STDERR to /dev/null: $!";
}

if ( $pidfile )
{
	my $fd = IO::File->new(">$pidfile")
		|| die "Unable to open pidfile: $pidfile: $!";
	$fd->write("$$");
	$fd->close();
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

