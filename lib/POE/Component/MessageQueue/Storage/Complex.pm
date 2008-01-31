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
use Moose;
with qw(POE::Component::MessageQueue::Storage::Double);
use POE;

has 'timeout' => (
	is       => 'ro',
	isa      => 'Int',
	default  => 4,
	required => 1,
);

has 'alias' => (
	is      => 'ro',
	default => 'MQ-Expire-Timer',
	required => 1,
);

has 'session' => (
	is      => 'ro',
	lazy    => 1,
	default => sub {
		my $self = shift;
		return POE::Session->create(
			inline_states => {
				'_start' => sub {$poe_kernel->alias_set($self->alias) },
				'_expire_message' => sub {
					my ($kernel, @args) = @_[KERNEL, ARG0, ARG1];
					$kernel->delay_set('_timer_up', $self->timeout, @args);
				},
				'_timer_up' => sub { $self->expire_message()->(@_[ARG0,ARG1]) },	
			},
		);
	},
);

has 'expire_message' => (
	is       => 'ro',
	isa      => 'CodeRef',
	required => 1,
	default  => sub { 
		my $self = shift;
		return sub {
			my ($message, $callback) = @_;

			$self->front->remove($message->{message_id}, sub {
				# The message may not be there if it was removed before it expired.
				my $message = shift;
				return unless $message;

				if ($message->{persistent}) 
				{
					$self->log('info', sprintf('Moving expired message %s into backstore',
						$message->{message_id}));
					$self->back->store($message, $callback);
				}
				elsif($callback)
				{
					$callback->($message);
				}
			});
		};
	},
);

override 'new' => sub {
	my $self = super();
	$self->children({FRONT => $self->front, BACK => $self->back});
	$self->add_names qw(COMPLEX);
	return $self;
};

sub store
{
	my ($self, $message, $callback) = @_;

	$self->front->store($message, sub {
		# Don't start ticking the clock until it finishes storing.
		my $message = shift;
		$poe_kernel->post($self->session, '_expire_message', $message, $callback);
		$callback->($message);
	});
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->alias);

	$self->log('alert', 'Forcing messages from frontstore to backstore');

	$self->front->remove_all(sub {
		my $message_aref = shift;
		my @messages = grep { $_->{persistent} } (@$message_aref);
		
		foreach my $msg (@messages)
		{
			$self->log('info', "Moving message $msg->{message_id} into backstore.");
			$self->back->store($msg, sub {});
		}	

		$self->front->storage_shutdown(sub {
			$self->back->storage_shutdown($complete)
		});
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
			front      => POE::Component::MessageQueue::Storage::BigMemory->new(),
			back       => POE::Component::MessageQueue::Storage::Throttled->new({
				storage => My::Persistent::But::Slow::Datastore->new(),	
			}),
			expire_message => sub {
				my ($self, $message, $callback) = @_;;
				do_something($message);
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
passed to expire_message. 

=item expire_message => CODEREF

A function of three arguments (the storage engine (self), a message, and
optionally a callback to call when the action is finished) that does something
with expired messages.  The default action is to delete them from the front
store and store them in the back-store, but you can override that here.

=item front => SCALAR

=item back => SCALAR

Takes a reference to a storage engine to use as the front store / back store.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::Complex::Default> for the most common
case.

=cut
