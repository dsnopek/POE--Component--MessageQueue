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
my $show_version = 0;
my $show_usage   = 0;

GetOptions(
	"port|p=i"     => \$port,
	"hostname|h=s" => \$hostname,
	"timeout|i=i"  => \$timeout,
	"throttle|T=i" => \$throttle_max,
	"data-dir=s"   => \$DATA_DIR,
	"log-conf=s"   => \$CONF_LOG,
	"background|b" => \$background,
	"pidfile|p=s"  => \$pidfile,
	"version|v"    => \$show_version,
	"help|h"       => \$show_usage,
);

sub version
{
	print "POE::Component::MessageQueue version $POE::Component::MessageQueue::VERSION\n";
	print "Copyright 2007 David Snopek\n";
}

sub usage
{
	my $X = ' ' x (length $0);

	print "$0 [--port|-p <num>] [--hostname|-h <host>]\n";
	print "$X [--timeout|-i <seconds>]   [--throttle|-T <count>]\n";
	print "$X [--data-dir <path_to_dir>] [--log-cont <path_to_file>]\n";
	print "$X [--background|-b] [--pidfile|-p <path_to_file>]\n";
	print "$X [--version|-v] [--help|-h]\n";

	print "\nSERVER OPTIONS:\n";
	print "  --port     -p <num>    The port number to listen on (Default: 61613)\n";
	print "  --hostname -h <host>   The hostname of the interface to listen on (Default: localhost)\n";

	print "\nSTORAGE OPTIONS:\n";
	print "  --timeout  -i <secs>   The number of seconds to keep messages in the front-store (Default: 4)\n";
	print "  --throttle -T <count>  The number of messages that can be stored at once before throttling (Default: 2)\n";
	print "  --data-dir <path>      The path to the directory to store data (Default: /var/lib/perl_mq)\n";
	print "  --log-conf <path>      The path to the log configuration file (Default: /etc/perl_mq/log.conf\n";
	print "\nDAEMON OPTIONS:\n";
	print "  --background -b        If specified the script will daemonize and run in the background\n";
	print "  --pidfile    -p <path> The path to a file to store the PID of the process\n";

	print "\nOTHER OPTIONS:\n";
	print "  --version    -v        Show the current version.\n";
	print "  --help       -h        Show this usage message\n";
}

if ( $show_version )
{
	version;
	exit 0;
}

if ( $show_usage )
{
	version;
	print "\n";
	usage;
	exit 0;
}

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

