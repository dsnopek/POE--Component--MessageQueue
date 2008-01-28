#
# Copyright 2007 David Snopek <dsnopek@gmail.com>
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
	my $expire = $args->{expire} ||
		sub {
			my ($message, $callback) = @_;

			$self->{front_store}->remove($message->{message_id}, sub {
				# The message may not be there if it was removed before it expired.
				my $message = shift;
				return unless $message;

				if ($message->{persistent}) 
				{
					$self->_log('info', 'STORE: COMPLEX: Moving expired message '.
						"$message->{message_id} into the backstore.");
					$self->{back_store}->store($message, $callback);
				}
				elsif($callback)
				{
					$callback->($message);
				}
			});
		};

	$self->{session_alias} = 'MQ-Expire-Timer';

	# our session that does the timed message check-up.
	$self->{session} = POE::Session->create(
		inline_states => {
			'_start' => sub{ $poe_kernel->alias_set($self->{session_alias}) },
			'_expire_message' => sub {
				my ($kernel, @args) = @_[KERNEL, ARG0, ARG1];
				$kernel->delay_set(_timer_up => $self->{timeout} => @args);
			},
			'_timer_up' => sub { $expire->(@_[ARG0,ARG1]) },	
		},
	);

	return bless $self, $class;
}

sub set_logger
{
	my $self = shift;
	$self->SUPER::set_logger(@_);
	$self->{front_store}->set_logger(@_);
	$self->{back_store}->set_logger(@_);
}

sub store
{
	my ($self, $message, $callback) = @_;

	$self->{front_store}->store($message, sub {
		# Don't start ticking the clock until it finishes storing.
		my $message = shift;
		$poe_kernel->post($self->{session}, '_expire_message', $message, $callback);
		$callback->($message);
	});
}

# For these remove functions, the control flow is the same, but the
# particulars vary.
# front, back: call remove on their respective store with their argument as
# the callback argument to remove.
# combine: a function that aggregates the results of front and back.
# callback: the thing to callback with the results of the remove op.
sub _remove_underneath
{
	my ($self, %args) = @_;

	# There are no results, so just call everything in parallel.
	unless ($args{callback}) 
	{
		$args{front}->();
		$args{back}->();
		return;
	}

	# Save the results of front and back, aggregate them, and pass them on.
	$args{front}->(sub {
		my $fresult = shift;
		$args{back}->(sub {
			my $bresult = shift;	
			$args{callback}->($args{combine}->($fresult, $bresult));
		});
	});
	return;
}

sub remove
{
	my ($self, $id, $cb) = @_;
	$self->_remove_underneath(
		front    => sub { $self->{front_store}->remove($id, shift) },
		back     => sub {  $self->{back_store}->remove($id, shift) },
		combine  => sub { $_[0] || $_[1] },
		callback => $cb,
	);
}

# Combine for _multiple and _all
sub __append
{
	my ($one, $two) = @_;
	push(@$one, @$two);
	return $one;
}

# We'll call remove_multiple on the full range of ids - well-behaved stores
# will just ignore IDs they don't have.
sub remove_multiple
{
	my ($self, $id_aref, $cb);
	$self->_remove_underneath(
		front    => sub { $self->{front_store}->remove_multiple($id_aref, shift) },
		back     => sub {  $self->{back_store}->remove_multiple($id_aref, shift) },
		combine  => \&__append,
		callback => $cb,
	);
}

sub remove_all
{
	my ($self, $cb);
	$self->_remove_underneath(
		front    => sub { $self->{front_store}->remove_all(shift) },
		back     => sub {  $self->{back_store}->remove_all(shift) },
		combine  => \&__append,
		callback => $cb,
	);	
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;
	my ($front, $back) = ($self->{front_store}, $self->{back_store});

	$front->claim_and_retrieve($destination, $client_id, sub {
		my $message = shift;
		$message ? $dispatch->($message, $destination, $client_id) :
		           $back->claim_and_retrieve($destination, $client_id, $dispatch);
	});
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;

	$self->{front_store}->disown( $destination, $client_id );
	$self->{back_store}->disown( $destination, $client_id );
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	return if $self->{shutdown};
	$self->{shutdown} = 1;

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->{session_alias});

	$self->_log('alert', 
		'Forcing all messages from the front-store into the back-store...'
	);

	my ($front, $back) = ($self->{front_store}, $self->{back_store});

	$front->remove_all(sub {
		my $message_aref = shift;
		my @messages = grep { $_->{persistent} } (@$message_aref);
		
		foreach my $msg (@messages)
		{
			$self->_log('info',
				"STORE: COMPLEX: Moving message $msg->{message_id} " .
				'into backing store.'
			);
			$back->store($msg, sub {});
		}	

		$front->storage_shutdown(sub {$back->storage_shutdown($complete)});
	});
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
			front_store => POE::Component::MessageQueue::Storage::BigMemory->new(),
			back_store => POE::Component::MessageQueue::Storage::Throttled->new({
				storage => My::Persistent::But::Slow::Datastore->new(),	
			}),
			expire => sub {
				my $message_id = shift;
				do_something($message_id);
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
passed to expire. 

=item expire => CODEREF

A function of one argument (a message id) that does something
with expired messages.  The default action is to delete them from the front
store and store them in the back-store, but you can override that here.

=item front_store => SCALAR

=item back_store => SCALAR

Takes a reference to a storage engine to use as the front store / back store.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::Complex::Default> for the most common
case.

=cut
