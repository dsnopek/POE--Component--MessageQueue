
package POE::Component::MessageQueue::Storage;

use POE::Component::MessageQueue::Logger;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	# a null logger
	my $logger = POE::Component::MessageQueue::Logger->new();

	my $self = {
		logger            => $logger,
		message_stored    => undef,
		dispatch_message  => undef,
		destination_ready => undef,
		# TODO: do something with this.
		started           => 0
	};

	bless  $self, $class;
	return $self;
}

sub _log
{
	my $self = shift;
	$self->{logger}->log(@_);
}

sub set_message_stored_handler
{
	my ($self, $handler) = @_;

	$self->{message_stored} = $handler;
}

sub set_dispatch_message_handler
{
	my ($self, $handler) = @_;
	
	$self->{dispatch_message} = $handler;
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;

	$self->{destination_ready} = $handler;
}

sub set_logger
{
	my ($self, $logger) = @_;

	$self->{logger} = $logger;
}

1;

