
use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Complex;
use strict;

my $DATA_DIR = '/tmp/perl_mq';

# we create a logger, because a production message queue would
# really need one.
POE::Component::Logger->spawn(
	ConfigFile => 'log.conf',
	Alias      => 'mq_logger'
);

POE::Component::MessageQueue->new({
	# configure to use a logger
	logger_alias => 'mq_logger',

	storage => POE::Component::MessageQueue::Storage::Complex->new({
		data_dir => $DATA_DIR,
		timeout  => 4
	})
});

POE::Kernel->run();
exit;

