#!/usr/bin/perl

use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Complex;
use strict;

my $DATA_DIR = '/var/lib/perl_mq';
my $CONF_DIR = '/etc/perl_mq';
my $CONF_LOG = "$CONF_DIR/log.conf";

if ( not -d $DATA_DIR )
{
	mkdir $DATA_DIR
		|| die "Unable to create: $DATA_DIR";
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
	storage => POE::Component::MessageQueue::Storage::Complex->new({
		data_dir => $DATA_DIR,
		timeout  => 4
	}),
	logger_alias => $logger_alias,
});

POE::Kernel->run();
exit;

