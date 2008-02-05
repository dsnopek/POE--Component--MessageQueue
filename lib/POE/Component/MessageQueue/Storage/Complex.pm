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

package POE::Component::MessageQueue::Storage::Complex;
use base qw(POE::Component::MessageQueue::Storage);

use POE;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $self = $class->SUPER::new( $args );

	$self->{timeout}     = $args->{timeout}    || die "No timeout.";
	$self->{front_store} = $args->{front_store}|| die "No front store.";
	$self->{back_store}  = $args->{back_store} || die "No back store.";

	# The default is to move persistent messages to the backstore and to discard
	# messages that are not persistent.	
	$self->{expire_messages} = $args->{expire_messages} ||
		sub {
			my $message_ids = shift;
			my @messages = grep { 
				$_->{persistent} 
			} (@{$self->{front_store}->remove_multiple($message_ids)});

			foreach (@messages) {
				$self->_log('info',
					'STORE: COMPLEX: ' .
					"Moving expired message $_->{message_id} into backing store"
				);
				$self->{back_store}->store($_);
			}
		};

	$self->{delay}      = int($self->{timeout} / 2);
	$self->{timestamps} = {};

	# our session that does the timed message check-up.
	$self->{session} = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->yield('_check_messages');
			},
		},
		object_states => [
			$self => [
				'_check_messages',
			],
		],
	);

	return bless $self, $class;
}

sub set_message_stored_handler 
{
	my $self = shift;
	$self->SUPER::set_message_stored_handler(@_);
	$self->{front_store}->set_message_stored_handler(@_);
	$self->{back_store}->set_message_stored_handler(@_);
}

sub set_dispatch_message_handler {
	my $self = shift;
	$self->SUPER::set_dispatch_message_handler(@_);
	$self->{front_store}->set_dispatch_message_handler(@_);
	$self->{back_store}->set_dispatch_message_handler(@_);
}

sub set_destination_ready_handler
{
	my $self = shift;
	$self->SUPER::set_destination_ready_handler(@_);
	$self->{front_store}->set_destination_ready_handler(@_);
	$self->{back_store}->set_destination_ready_handler(@_);
}

sub set_logger
{
	my $self = shift;
	$self->SUPER::set_logger(@_);
	$self->{front_store}->set_logger(@_);
	$self->{back_store}->set_logger(@_);
}

sub set_shutdown_complete_handler
{
	my ($self, $handler) = @_;
	$self->{back_store}->set_shutdown_complete_handler( $handler );
}

sub store
{
	my ($self, $message) = @_;

	$self->{front_store}->store( $message );

	# mark the timestamp that this message was added 
	# We ignore the persistent flag, because it's up to expire_message to decide
	# what to do with that. 
	$self->{timestamps}->{$message->{message_id}} = time();
}

sub remove
{
	my ($self, $message_id) = @_;

	$self->{front_store}->remove( $message_id ) ||
		$self->{back_store}->remove( $message_id );

	delete $self->{timestamps}->{$message_id};
}

sub claim_and_retrieve
{
	my $self = shift;

	return $self->{front_store}->claim_and_retrieve(@_) || 
		$self->{back_store}->claim_and_retrieve(@_);
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;

	$self->{front_store}->disown( $destination, $client_id );
	$self->{back_store}->disown( $destination, $client_id );
}

# our periodic check to move messages into the backing store
sub _check_messages
{
	my ($self, $kernel) = @_[ OBJECT, KERNEL ];

	return if $self->{shutdown};

	$self->_log( 'debug', 'STORE: COMPLEX: Checking for outdated messages' );

	my $threshold = time() - $self->{timeout};
	my @outdated = grep {
		$self->{timestamps}->{$_} < $threshold
	} (keys %{$self->{timestamps}});

	$self->{expire_messages}->(\@outdated);
	delete $self->{timestamps}->{$_} for (@outdated);

	# Set timer for next expiration check.
	$kernel->delay( '_check_messages', $self->{delay} );
}

sub shutdown
{
	my $self = shift;

	return if $self->{shutdown};
	$self->{shutdown} = 1;

	# shutdown our check messages session
	$poe_kernel->signal( $self->{session}, 'TERM' );

	$self->_log('alert', 
		'Forcing all messages from the front-store into the back-store...'
	);

	my @messages = grep {
		$_->{persistent} 
	} (@{$self->{front_store}->empty_all()});

	foreach (@messages) {
		$self->_log('info',
			"STORE: COMPLEX: Moving message $_->{message_id} " .
			'into backing store.'
		);
		$self->{back_store}->store($_);
	}

	$self->{front_store}->shutdown();
	$self->{back_store}->shutdown();
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Complex -- A configurable storage
engine that keeps a front-store (something fast) and a back-store 
(something persistent), allowing you to specify a timeout and an action to be 
taken when messages in the front-store expire. 

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Complex;
  use strict;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Complex->new({
      timeout      => 4,
      throttle_max => 2,

      front_store => POE::Component::MessageQueue::Storage::Memory->new(),
      # Or, an alternative memory store is available!
      #front_store => POE::Component::MessageQueue::Storage::BigMemory->new(),

      back_store => POE::Component::MessageQueue::Storage::Throttled->new({
        storage => My::Persistent::But::Slow::Datastore->new()

        # Examples include:
        #storage => POE::Component::MessageQueue::Storage::DBI->new({ ... });
        #storage => POE::Component::MessageQueue::Storage::FileSystem->new({ ... });
      }),

      # Optional: Action to perform on after timeout.  By default moves all
      # persistent messages into the backstore.
      expire_messages => sub {
        my $arrayref_of_message_ids = shift;
        do_something($arrayref_of_message_ids);
      },
    })
  });

POE::Kernel->run();
exit;

=head1 DESCRIPTION

The idea of having a front store (something quick) and a back store (something
persistent) is common and recommended, so this class exists as a helper to
implementing that pattern.  It wraps any front and back store that you
specify, a timeout that you specify, and tells you when messages expire.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item timeout => SCALAR

The number of seconds after a message enters the front-store before it
expires.  After this time, if the message hasn't been removed, it will be
passed to expire_messages. 

=item expire_messages => CODEREF

A function of one argument (an arrayref of message ids) that does something
with expired messages.  The default action is to delete them from the front
store and store them in the back-store, but you can override that here.

=item front_store => SCALAR

Takes a reference to a storage engine to use as the front store.

Currently, only the following storage engines are capable to be front stores:

=over 2

=item *

L<POE::Component::MessageQueue::Storage::Memory>

=item *

L<POE::Component::MessageQueue::Storage::BigMemory>

=back

Expect this to change in future versions.

=item back_store => SCALAR

Takes a reference to a storage engine to use as the back store.

Using L<POE::Component::MessageQueue::Storage::Throttled> to wrap your main
storage engine is highly recommended for the reasons explained in its specific
documentation.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::Complex::Default> - The most common case.  Based on this storage engine.

I<External references:>

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<DBI>,
L<DBD::SQLite>

I<Other storage engines:>

L<POE::Component::MessageQueue::Storage::Default>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::BigMemory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>
L<POE::Component::MessageQueue::Storage::Default>

=cut
