#
# Copyright 2007, 2008 David Snopek <dsnopek@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

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
		shutdown_complete => undef,
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
	undef;
}

sub set_dispatch_message_handler
{
	my ($self, $handler) = @_;
	$self->{dispatch_message} = $handler;
	undef;
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;
	$self->{destination_ready} = $handler;
	undef;
}

sub set_shutdown_complete_handler
{
	my ($self, $handler) = @_;
	$self->{shutdown_complete} = $handler;
	undef;
}

sub set_logger
{
	my ($self, $logger) = @_;
	$self->{logger} = $logger;
	undef;
}

# A hack to allow POE::Component::Generic to set the log function
# in a single event.  This allows us to setup the logger before any
# other events happen.
sub set_log_function
{
	my ($self, $func) = @_;
	$self->get_logger()->set_log_function($func);
	undef;
}

sub get_logger
{
	my $self = shift;
	return $self->{logger};
}

sub store
{
	my ($self, $message) = @_;

	die "Abstract.";
}

sub remove
{
	my ($self, $message_id) = @_;

	die "Abstract.";
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	my $destination;
	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
		$client_id   = $args->{client_id};
	}
	else
	{
		$destination = $args;
		$client_id   = shift;
	}

	die "Abstract.";
}

sub disown
{
	my ($self, $destination, $client_id) = @_;

	die "Abstract.";
}

# a semi-hidden alias that we need when using a storage engine
# behind POE::Component::MessageQueue::Storage::Generic!
sub storage_shutdown
{
	my $self = shift;
	$self->shutdown();
}

sub shutdown
{
	my $self = shift;

	die "Abstract.";
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage -- Parent of provided storage engines

=head1 DESCRIPTION

The parent class of the provided storage engines.  This is an "abstract" class that can't be used as is, but defines the interface for other objects of this type.

=head1 INTERFACE

=over 2

=item set_message_stored_handler I<CODEREF>

Takes a CODEREF which will get called back when a message has been successfully stored.  This functwion will be called with one argument, the name of the destination.

=item set_dispatch_message_handler I<CODEREF>

Takes a CODEREF which will get called back when a message has been retrieved from the store.  This will be called with three arguments: the message, the destination string, and the client id.  If no message could be retrieved the function will still be called but with the message undefined.

=item set_destination_ready_handler I<CODEREF>

Takes a CODEREF which will get called back when a destination is ready to be claimed from again.  This is necessary for storage engines that will lock a destination while attempting to retrieve a message.  This handler will be called when the destination is unlocked so that message queue knows that it can claim more messages.  If your storage engine doesn't lock anything, you B<must> call this handler immediately after called the above handler.  

It will be called with a single argument: the destination string.

=item set_shutdown_complete_handler I<CODEREF>

Takes a CODEREF which will get called when the storage engine has finished shutting down.  The shutdown process is started by calling I<shutdown()> on the storage engine (see below).

It will be called without any arguments.

=item set_logger I<SCALAR>

Takes an object of type L<POE::Component::MessageQueue::Logger> that should be used for logging.

=item get_next_message_id

Should return the next available message_id.

=item store I<SCALAR>

Takes an object of type L<POE::Component::MessageQueue::Message> that should be stored.  This call will eventually result in the I<message_stored_handler> being called exactly once.

=item remove I<SCALAR>

Takes a message_id to be removed from the storage engine.

=item claim_and_retrieve I<SCALAR, SCALAR> or I<HASHREF>

Takes the destination string and client id (or a HASHREF with keys "destination" and "client_id").  Should claim a message for the given client id on the given destination.  This call will eventually result in the I<dispatch_message_handler> and I<destination_ready_handler> being called exactly once each.

=item disown I<SCALAR>, I<SCALAR>

Takes a destination and client id.  All messages which are owned by this client id on this destination should be marked as owned by nobody.

=item shutdown

Will start shutting down the storage engine.  The I<shutdown_complete> handler will be called when the storage engine has finished shutting down.  This should be a graceful operation.  Ie., The storage engine will attempt to clean-up and push messages to persistent storage if possible before calling the I<shutdown_complete> handler.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut
