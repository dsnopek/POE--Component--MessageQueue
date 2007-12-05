
package POE::Component::MessageQueue::Storage::Generic::Base;
use base qw(POE::Component::MessageQueue::Storage);
use POE::Component::MessageQueue;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $self = $class->SUPER::new( $args );

	# We're in a child process when this happens: if we don't do this, we'll get
	# killed on these signals and PoCo::MQ::Storage::Generic will get a broken
	# pipe when it tries to talk to us.
	foreach my $sig (POE::Component::MessageQueue->SHUTDOWN_SIGNALS) {
		$SIG{$sig} = 'IGNORE';
	} 

	bless  $self, $class;
	return $self;
}

1;
