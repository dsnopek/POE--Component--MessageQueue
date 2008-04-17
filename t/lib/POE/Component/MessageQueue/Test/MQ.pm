package POE::Component::MessageQueue::Test::MQ;
use strict;
use warnings;
use Exporter qw(import);
our @EXPORT = qw(start_mq stop_mq);

sub start_mq {
	my $pid      = fork;
	my $storage  = shift || 'Memory';
	return $pid if $pid;

	use POE;
	use POE::Component::MessageQueue;
	use POE::Component::MessageQueue::Logger;
	use POE::Component::MessageQueue::Storage::Memory;
	use POE::Component::MessageQueue::Test::EngineMaker;

	my %defaults = (
		port    => 8099,
		storage => make_engine($storage),
		logger  => POE::Component::MessageQueue::Logger->new(level=>7),
	);

	my %options = @_;
	$defaults{$_} = $options{$_} foreach (keys %options);

	POE::Component::MessageQueue->new(%defaults);

	$poe_kernel->run();
	exit 0;
}

sub stop_mq {
	my $pid = shift;
	
	return (kill('TERM', $pid) == 1)
		&& (waitpid($pid, 0) == $pid)
		&& ($? == 0);
}

1;
