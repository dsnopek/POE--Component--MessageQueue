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

my $port     = 61613;
my $hostname = undef;
my $timeout  = 4;
my $throttle_max = 2;
my $background = 0;
my $debug_shell = 0;
my $pidfile;
my $show_version = 0;
my $show_usage   = 0;
my $statistics   = 0;
my $stat_interval = 10;

GetOptions(
	"port|p=i"     => \$port,
	"hostname|h=s" => \$hostname,
	"timeout|i=i"  => \$timeout,
	"throttle|T=i" => \$throttle_max,
	"data-dir=s"   => \$DATA_DIR,
	"log-conf=s"   => \$CONF_LOG,
	"stats!"       => \$statistics,
	"stats-interval=i" => \$stat_interval,
	"background|b" => \$background,
	"debug-shell"  => \$debug_shell,
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
    print <<"ENDUSAGE";
$0 [--port|-p <num>] [--hostname|-h <host>]
$X [--timeout|-i <seconds>]   [--throttle|-T <count>]
$X [--data-dir <path_to_dir>] [--log-conf <path_to_file>]
$X [--stats] [--stats-interval|-i <seconds>]
$X [--background|-b] [--pidfile|-p <path_to_file>]
$X [--debug-shell] [--version|-v] [--help|-h]

SERVER OPTIONS:
  --port     -p <num>     The port number to listen on (Default: 61613)
  --hostname -h <host>    The hostname of the interface to listen on 
                          (Default: localhost)

STORAGE OPTIONS:
  --timeout  -i <secs>    The number of seconds to keep messages in the 
                          front-store (Default: 4)
  --throttle -T <count>   The number of messages that can be stored at once 
                          before throttling (Default: 2)
  --data-dir <path>       The path to the directory to store data 
                          (Default: /var/lib/perl_mq)
  --log-conf <path>       The path to the log configuration file 
                          (Default: /etc/perl_mq/log.conf

STATISTICS OPTIONS:
  --stats                 If specified the, statistics information will be 
                          written to \$DATA_DIR/stats.yml
  --stats-interval <secs> Specifies the number of seconds to wait before 
                          dumping statistics (Default: 10)

DAEMON OPTIONS:
  --background -b         If specified the script will daemonize and run in the
                          background
  --pidfile    -p <path>  The path to a file to store the PID of the process

OTHER OPTIONS:
  --debug-shell           Run with POE::Component::DebugShell
  --version    -v         Show the current version.
  --help       -h         Show this usage message

ENDUSAGE
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

my %args = (
	port     => $port,
	hostname => $hostname,

	storage => POE::Component::MessageQueue::Storage::Complex->new({
		data_dir     => $DATA_DIR,
		timeout      => $timeout,
		throttle_max => $throttle_max
	}),

	logger_alias => $logger_alias,
);
if ($statistics) {
	require POE::Component::MessageQueue::Statistics;
	require POE::Component::MessageQueue::Statistics::Publish::YAML;
	my $stat = POE::Component::MessageQueue::Statistics->new();
	my $publish = POE::Component::MessageQueue::Statistics::Publish::YAML->spawn(
		statistics => $stat,
		output => "$DATA_DIR/stats.yml",
		interval => $stat_interval,
	);
	$args{observers} = [ $stat ];
}
my $mq = POE::Component::MessageQueue->new(\%args);

# install the debug shell if requested
if ( $debug_shell )
{
	require POE::Component::DebugShell;
	POE::Component::DebugShell->spawn();
}

# install a die handler so we can catch crashes and log them
$SIG{__DIE__} = sub {
	my $trace = Devel::StackTrace->new;

	# attempt to write to the crashed log for later debugging
	my $fn = "$DATA_DIR/crashed.log";
	my $fd = IO::File->new(">>$fn");
	if ( $fd )
	{
		my (@l) = localtime(time());
		$fd->write("\n============================== \n");
		$fd->write(" Crashed: ".strftime('%Y-%m-%d %H:%M:%S', @l));
		$fd->write("\n============================== \n\n");
		$fd->write( $trace->as_string );
		$fd->close();
	}
	else
	{
		print STDERR "Unable to open crashed log '$fn': $!\n";
	}

	# spit out a stack trace
	print STDERR $trace->as_string;
};

POE::Kernel->run();
exit;

