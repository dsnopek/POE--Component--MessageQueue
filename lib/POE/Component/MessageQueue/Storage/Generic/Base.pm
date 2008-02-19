package POE::Component::MessageQueue::Storage::Generic::Base;
use Moose::Role;

# Exclude log cause we have our own - we want to call our setted postback.
with 'POE::Component::MessageQueue::Storage' => { excludes => 'log' };

sub log
{
	my $self = shift;;
	$self->log_function->(@_) if $self->has_log_function;
	return;
}

has 'log_function' => (
	is        => 'rw',
	writer    => 'set_log_function',
	predicate => 'has_log_function',
);

sub ignore_signals
{
	my ($self, @signals) = @_;
	$SIG{$_} = 'IGNORE' foreach (@signals);
}

1;
