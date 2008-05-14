package POE::Component::MessageQueue::Test::MQ;
use strict;
use warnings;

use POE::Component::MessageQueue::Test::ForkRun;
use Exporter qw(import);
our @EXPORT = qw(start_mq stop_fork);

sub start_mq {
	my %options = @_;
	my $storage = delete $options{storage} || 'BigMemory';
	start_fork(sub {
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

		$defaults{$_} = $options{$_} foreach (keys %options);

		POE::Component::MessageQueue->new(%defaults);
	});
}

1;
