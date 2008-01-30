package POE::Component::MessageQueue::Storage::Generic::Base;
use Moose::Role;
with qw(POE::Component::MessageQueue::Storage);
use POE::Component::MessageQueue;

after 'new' => sub {
	foreach my $sig (POE::Component::MessageQueue->SHUTDOWN_SIGNALS) {
		$SIG{$sig} = 'IGNORE';
	} 
};

# Generics have funny behavior, and we'd like to be able to set the logger on
# a single event.  So, we have this special call, ONLY for generics.
sub set_log_function
{
	my ($self, $fn) = @_;
	$self->get_logger()->set_log_function($fn)
}

1;
