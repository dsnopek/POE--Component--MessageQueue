
use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Complex;
use POE::Component::MessageQueue::Logger;
use Getopt::Long;
use strict;

# Force some logger output without using the real logger.
$POE::Component::MessageQueue::Logger::LEVEL = 0;

my $DATA_DIR = '/tmp/perl_mq';

my $port     = 61613;
my $hostname = undef;

GetOptions(
	"port|p=i"     => \$port,
	"hostname|h=s" => \$hostname
);

# we create a logger, because a production message queue would
# really need one.
POE::Component::Logger->spawn(
	ConfigFile => 'log.conf',
	Alias      => 'mq_logger'
);

POE::Component::MessageQueue->new({
	port     => $port,
	hostname => $hostname,

	# configure to use a logger
	logger_alias => 'mq_logger',

	storage => POE::Component::MessageQueue::Storage::Complex->new({
		data_dir => $DATA_DIR,
		timeout  => 4
	})
});

POE::Kernel->run();
exit;

