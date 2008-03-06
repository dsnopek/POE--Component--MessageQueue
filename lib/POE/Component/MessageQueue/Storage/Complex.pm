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
use Moose;
use MooseX::AttributeHelpers;
with qw(POE::Component::MessageQueue::Storage::Double);
use POE;

# Use Time::HiRes's time() if available (more accurate timeouts)
BEGIN { eval { require Time::HiRes; Time::HiRes->import qw(time); } };

has timeout => (
	is       => 'ro',
	isa      => 'Int',
	required => 1,
);

has granularity => (
	is       => 'ro',
	isa      => 'Int',
	required => 1,
);

has alias => (
	is       => 'ro',
	default  => 'MQ-Expire-Timer',
	required => 1,
);

has front_size => (
	metaclass => 'Number',
	is        => 'rw',
	isa       => 'Int',
	default   => 0,
	provides  => {
		'add' => 'more_front',
		'sub' => 'less_front',
	},
);

has front_max => (
	is       => 'ro',
	isa      => 'Int', 
	required => 1,
);

# All messages that currently reside in the front-store have timestamps.  If
# they've been persisted, their timestamps are zero.
has timestamps => (
	metaclass => 'Collection::Hash',
	is        => 'ro', 
	isa       => 'HashRef[Num]',
	default   => sub { {} },
	provides  => {
		'set'    => 'set_timestamp',
		'get'    => 'get_timestamp',
		'delete' => 'delete_timestamp',
		'clear'  => 'clear_timestamps',
		'exists' => 'has_timestamp',
		'kv'     => 'timestamp_pairs',
	},	
);

has shutting_down => (
	is       => 'rw',
	default  => 0, 
);

around remove => sub {
	my $original = shift;
	my ($self, $arg, $callback) = @_;
	# We're wrapping Storage's wrapped method, so $arg can still be either
	my $aref = (ref $arg eq 'ARRAY') ? $arg : [$arg];

	my @front_ids = grep $self->has_timestamp($_), @$aref;

	# We can avoid a storage call if none of these are in front
	goto $original unless (@front_ids > 0);

	$self->front->get(\@front_ids, sub {
		my $messages = $_[0];
		foreach my $msg (@$messages)
		{
			$self->delete_timestamp($msg->id);
			$self->less_front($msg->size);
		}
		@_ = ($self, $aref, $callback);
		goto $original;
	});
};

before empty => sub {
	my ($self) = @_;
	$self->clear_timestamps();
	$self->front_size(0);
};

make_immutable;

sub BUILD 
{
	my $self = shift;
	POE::Session->create(
		object_states => [ $self => [qw(_start _expiration_check _push_messages)] ],
	);
	$self->children({FRONT => $self->front, BACK => $self->back});
	$self->add_names qw(COMPLEX);
}

# Remove oldest messages, storing them in the backstore if they haven't
# expired yet.
sub _bump_messages
{
	my ($self, $callback) = @_;
	if ($self->front_size <= $self->front_max)
	{
		goto $callback if $callback;	
	}
	else
	{
		$self->front->get_oldest(sub {
			my $message = $_[0];
			my $again = sub {
				@_ = ($self, $callback);
				goto &_bump_messages;
			};

			$self->less_front($message->size);

			my $timestamp = $self->delete_timestamp($message->id);

			$self->log('info', 'Message '.$message->id.' bumped from frontstore.');
			$self->front->remove($message->id, 
				$timestamp ? 
				sub { $self->back->store($message, $again) } : 
				$again
			);
		});
	}
}

sub store
{
	my ($self, $message, $callback) = @_;

	$self->front->store($message, sub {
		$self->more_front($message->size);
		my $stamp = ($message->persistent ? time() : 0);
		$self->set_timestamp($message->id, $stamp);
		$poe_kernel->post($self->alias, '_expiration_check');
		$self->_bump_messages($callback);
	});
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$poe_kernel->alias_set($self->alias);
}

sub expire_messages
{
	my ($self, $message_ids, $callback) = @_;

	my $idstr = join(', ', @$message_ids);
	$self->log(info => "Pushing expired messages ($idstr) to backstore.");
	$self->set_timestamp(map {($_ => 0)} @$message_ids);
	$self->front->get($message_ids, sub {
		$poe_kernel->post($self->alias, _push_messages => $_[0], $callback);
	});
}

sub _expiration_check
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];

	return if $self->shutting_down;

	my $thresh = time() - $self->timeout;

	my @expired = map  {$_->[0]} 
	              grep {my $s = $_->[1]; $s > 0 && $s < $thresh} 
	              $self->timestamp_pairs;

	# We want to avoid checking again until the expires from the last check have
	# finished, so we wait to set the alarm.
	my $at = time() + $self->granularity;
	my $next_check = sub { $kernel->alarm(_expiration_check => $at) };

	# Just set the alarm unless we have some expired messages
	goto $next_check unless (@expired > 0);

	$self->expire_messages(\@expired, $next_check);
}

sub _push_messages
{
	my ($self, $messages, $callback) = @_[OBJECT, ARG0..ARG1];
	if(my $msg = pop(@$messages))
	{
		$self->back->store($msg, sub {
			$poe_kernel->post($self->alias, _push_messages => $messages, $callback);
		});
	}
	else
	{
		goto $callback;
	}
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->shutting_down(1);

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->alias);

	$self->log('alert', 'Forcing messages from frontstore to backstore');

	$self->front->get_all(sub {
		my $message_aref = $_[0];

		# Persistent messages that have a non-zero timestamp.
		my @messages = grep {$_->persistent && $self->get_timestamp($_->id)} 
		               @$message_aref;
		
		my $idstr = join(', ', map $_->id, @messages);
		$self->log(info => "Moving messages $idstr into backstore.");
		$poe_kernel->post($self->alias, _push_messages => \@messages, sub {
			$self->front->empty(sub {
				$self->front->storage_shutdown(sub {
					$self->back->storage_shutdown($complete)
				});
			});
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
taken when messages in the front-store expire.  If a different behavior is
desired after timeout expiration, subclass and override "expire_messages".

=head1 SYNOPSIS

	use POE;
	use POE::Component::MessageQueue;
	use POE::Component::MessageQueue::Storage::Complex;
	use strict;

	POE::Component::MessageQueue->new({
		storage => POE::Component::MessageQueue::Storage::Complex->new({
			timeout      => 4,
			granularity  => 2,
			throttle_max => 2,
			front      => POE::Component::MessageQueue::Storage::BigMemory->new(),
			back       => POE::Component::MessageQueue::Storage::Throttled->new({
				storage => My::Persistent::But::Slow::Datastore->new(),	
			}),
		})
	});

	POE::Kernel->run();
	exit;

=head1 DESCRIPTION

The idea of having a front store (something quick) and a back store (something
persistent) is common and recommended, so this class exists as a helper to
implementing that pattern.  It wraps any front and back store that you
specify, a timeout that you specify, and moves messages from front to back
when the timeout expires.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item timeout => SCALAR

The number of seconds after a message enters the front-store before it
expires.  After this time, if the message hasn't been removed, it will be
moved into the backstore.

=item granularity => SCALAR

The number of seconds to wait between checks for timeout expiration.

=item front => SCALAR

Takes a reference to a storage engine to use as the front store.

Currently, only the following storage engines are capable to be front stores:

=over 2

=item *

L<POE::Component::MessageQueue::Storage::Memory>

=item *

L<POE::Component::MessageQueue::Storage::BigMemory>

=back

Expect this to change in future versions.

=item back => SCALAR

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
