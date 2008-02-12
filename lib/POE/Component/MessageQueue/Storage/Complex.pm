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
	default => sub {
		my $self = shift;
		return POE::Session->create(
			object_states => [ $self => [qw(_start _expiration_check)] ],
		);
	},
);

has 'timestamps' => (
	is => 'ro',
	isa => 'HashRef',
	default => sub { {} },
);

has 'shutting_down' => (
	is       => 'rw',
	isa      => 'Int',
	default  => 0, 
);

before 'remove' => sub {
	my ($self, $aref) = @_;
	delete $self->timestamps->{$_} foreach (@$aref);
};

before 'empty' => sub {
	my ($self) = @_;
	%{$self->timestamps} = ();
};

make_immutable;

sub BUILD 
{
	my $self = shift;
	$self->children({FRONT => $self->front, BACK => $self->back});
	$self->add_names qw(COMPLEX);
}

sub store
{
	my ($self, $message, $callback) = @_;

	$self->front->store($message, sub {
		my $message = shift;
		$self->timestamps->{$message->id} = time();
		$callback->($message);
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
	$self->front->remove($message_ids, sub {
		my $aref = shift;
		foreach my $msg (@$aref)
		{
			# It's possible for a message to get removed midway through expiring, in
			# which case msg would be undefined.
			next unless $msg;

			delete $self->timestamps->{$msg->id};
			if ($msg->persistent)
			{
				$self->log('info', 
					sprintf('Moving expired message %s into backstore', $msg->id));
			
				$self->back->store($msg);
			}
		}
	});
}

sub _expiration_check
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];

	return if $self->shutting_down;

	$self->log('debug', 'Checking for outdated messages...');

	my $threshold = time() - $self->timeout;
	my @expired = grep { $self->timestamps->{$_} < $threshold } 
	                   (keys %{$self->timestamps});

	$self->expire_messages(\@expired) if (@expired > 0);
		
	$kernel->delay_set('_expiration_check', 1);
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->shutting_down(1);

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->alias);

	$self->log('alert', 'Forcing messages from frontstore to backstore');

	$self->front->empty(sub {
		my $message_aref = shift;
		my @messages = grep { $_->persistent } (@$message_aref);
		
		foreach my $msg (@messages)
		{
			$self->log('info', 
				sprintf("Moving message %s into backstore.", $msg->id));
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
