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
with qw(POE::Component::MessageQueue::Storage::Double);
use POE;

# Use Time::HiRes's time() if available (more accurate timeouts)
BEGIN { eval { require Time::HiRes; Time::HiRes->import qw(time); } };

has 'timeout' => (
	is       => 'ro',
	isa      => 'Int',
	required => 1,
);

has 'granularity' => (
	is       => 'ro',
	isa      => 'Int',
	required => 1,
);

has 'alias' => (
	is       => 'ro',
	default  => 'MQ-Expire-Timer',
	required => 1,
);

has 'session' => (is => 'rw');

has 'front_size' => (
	is      => 'rw',
	default => 0,
);

has 'front_max' => (
	is       => 'ro',
	isa      => 'Int', 
	required => 1,
);

# All messages that currently reside in the front-store have their ids as keys
# in this hash.  If the value is zero, the message has already been stored in
# the backstore.  Otherwise, it is the time() that this message was
# stored in the front-store.  You can tell if a message is in the front store
# by checking for the existence of its ID in this hash.
has 'timestamps' => (
	is      => 'ro', 
	default => sub { {} },
);

has 'shutting_down' => (
	is       => 'rw',
	default  => 0, 
);

around 'remove' => sub {
	my $original = shift;
	my ($self, $aref, $callback) = @_;
	my $front_ids = [grep { exists $self->timestamps->{$_} } @$aref];
	$self->front->get($front_ids, sub {
		my $messages = $_[0];
		my $total = $self->front_size;
		foreach my $msg (@$messages)
		{
			delete $self->timestamps->{$msg->id};
			$total -= $msg->size;
		}
		$self->front_size($total);		
		@_ = ($self, $aref, $callback);
		goto $original;
	});
};

before 'empty' => sub {
	my ($self) = @_;
	%{$self->timestamps} = ();
	$self->front_size(0);
};

make_immutable;

sub BUILD 
{
	my $self = shift;
	$self->session(POE::Session->create(
		object_states => [ $self => [qw(_start _expiration_check)] ],
	));
	$self->children({FRONT => $self->front, BACK => $self->back});
	$self->add_names qw(COMPLEX);
}

# Remove oldest messages, storing them in the backstore if they haven't
# expired yet.
sub _bump_messages
{
	my $self = $_[0];
	return unless ($self->front_size > $self->front_max);
	$self->front->get_oldest(sub {
		my $message = $_[0];
		my $callback;

		$self->front_size($self->front_size - $message->size);

		my $timestamp = delete $self->timestamps->{$message->id};
		$callback = sub {$self->back->store($message)} if ($timestamp);

		$self->log('info', 'Message '.$message->id.' bumped from frontstore.');
		$self->front->remove($message->id, $callback);

		@_ = ($self);
		goto &_bump_messages;
	});
}

sub store
{
	my ($self, $message, $callback) = @_;

	$self->front->store($message, sub {
		$self->front_size($self->front_size + $message->size);
		$self->timestamps->{$message->id} = $message->persistent ? time() : 0; 
		$self->_bump_messages();
		goto $callback if $callback;
	});
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$poe_kernel->alias_set($self->alias);
	$kernel->yield('_expiration_check');
}

sub expire_messages
{
	my ($self, $message_ids) = @_;
	$self->front->get($message_ids, sub {
		my $aref = shift;
		my @ids;
		foreach my $msg (@$aref)
		{
			push(@ids, $msg->id);
			$self->timestmaps->{$msg->id} = 0;
			$self->back->store($msg);
		}
		my $idstr = join(', ', @ids);
		$self->log(info => "Pushed expired messages ($idstr) to backstore.");
	});
}

sub _is_expired
{
	my ($self, $id, $threshold) = @_;
	my $stamp = $self->timestamps->{$id};
	return $stamp && $stamp < $threshold;
}

sub _expiration_check
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];

	return if $self->shutting_down;

	$self->log('debug', 'Checking for outdated messages...');

	my $t = time() - $self->timeout;
	my @expired = grep { $self->_is_expired($_, $t) } (keys %{$self->timestamps});

	$self->expire_messages(\@expired) if (@expired > 0);
		
	$kernel->delay_set('_expiration_check', $self->granularity);
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->shutting_down(1);

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->alias);

	$self->log('alert', 'Forcing messages from frontstore to backstore');

	$self->front->get_all(sub {
		my $message_aref = shift;
		my @messages = grep { 
			$_->persistent && $self->timestamps->{$_->id} 
		} (@$message_aref);
		
		my $idstr = join(', ', map $_->id, @$message_aref);
		$self->log(info => "Moving messages $idstr into backstore.");
		$self->back->store($message_aref, sub {
			$self->front->storage_shutdown(sub {
				$self->back->storage_shutdown($complete)
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
