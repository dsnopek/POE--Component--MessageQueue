package POE::Component::MessageQueue::Storage::Generic::Base;
use Moose::Role;
use POE::Component::MessageQueue;

# In generics, we just want log to call the postback (we have to do this
# before "with" injects a log method in here.
override 'log' => sub {
	my $self = shift;
	$self->log_function->(@_) if $self->has_logger;
};

with qw(POE::Component::MessageQueue::Storage);

has 'log_function' => (
	is        => 'rw',
	writer    => 'set_log_function',
	predicate => 'has_logger',
);

sub BEGIN
{
	foreach my $sig (POE::Component::MessageQueue->SHUTDOWN_SIGNALS) 
	{
		$SIG{$sig} = 'IGNORE';
	} 
}

1;
