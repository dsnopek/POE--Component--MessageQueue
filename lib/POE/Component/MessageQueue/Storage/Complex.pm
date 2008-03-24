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

package POE::Component::MessageQueue::Storage::Complex::IdleElement;
use Heap::Elem;
use base qw(Heap::Elem);

sub new
{
	my ($class, $id) = @_;
	my $self = bless([ @{ $class->SUPER::new($id) }, time() ], $class);
}

sub cmp
{
	my ($self, $other) = @_;
	return $self->[2] <=> $other->[2];
}

1;


package POE::Component::MessageQueue::Storage::Complex;
use Moose;
use MooseX::AttributeHelpers;
with qw(POE::Component::MessageQueue::Storage::Double);

use POE;
use Heap::Fibonacci;
use List::MoreUtils qw(zip);
use List::Util qw(sum);
eval q(use Time::HiRes qw(time)); # more accurate time() if available

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

# Messages which have not yet been expired to the backstore have a storage
# timestamp in this hash.
has expirations => (
	metaclass => 'Collection::Hash',
	is        => 'ro', 
	isa       => 'HashRef[Num]',
	default   => sub { {} },
	provides  => {
		'set'    => 'set_expiration',
		'delete' => 'delete_expiration',
		'clear'  => 'clear_expirations',
		'kv'     => 'expiration_pairs',
	},	
);

has idle_hash => (
	metaclass => 'Collection::Hash',
	is => 'ro',
	isa => 'HashRef',
	default   => sub { {} },
	provides  => {
		'set'    => '_hashset_idle',
		'get'    => 'get_idle',
		'delete' => 'delete_idle',
		'clear'  => 'clear_idle',
	},
);

has idle_heap => (
	is => 'ro',
	isa => 'Heap::Fibonacci',
	lazy => 1,
	default => sub { Heap::Fibonacci->new },
	clearer => 'reset_idle_heap',
);

sub set_idle
{
	my ($self, @ids) = @_;
	my @elems = map {
		POE::Component::MessageQueue::Storage::Complex::IdleElement->new($_);
	} @ids;
	$self->_hashset_idle(zip @ids, @elems);
	$self->idle_heap->add($_) foreach @elems;
}

around delete_idle => sub {
	my $original = shift;
	$_[0]->idle_heap->delete($_) foreach ($original->(@_));
};

after clear_idle => sub {$_[0]->reset_idle_heap()};

has shutting_down => (
	is       => 'rw',
	default  => 0, 
);

after remove => sub {
	my ($self, $arg, $callback) = @_;
	my $aref = (ref $arg eq 'ARRAY') ? $arg : [$arg];
	my @ids = (grep $self->in_front($_), @$aref) or return;

	$self->delete_idle(@ids);
	$self->delete_expiration(@ids);

	$self->less_front(sum map {$_->{size}} 
	                      grep {$_}
	                      $self->delete_front(@ids))
};

after empty => sub {
	my ($self) = @_;
	$self->clear_front();
	$self->clear_idle();
	$self->clear_expirations();
	$self->front_size(0);
};

after $_ => sub {$_[0]->_activity($_[1])} foreach qw(claim get);

around claim_and_retrieve => sub {
	my $original = shift;
	my $self = $_[0];
	my $callback = pop;
	$original->(@_, sub {
		if (my $msg = $_[0])
		{
			$self->_activity($msg->id);
		}
		goto $callback;
	});
};

make_immutable;

sub _activity
{
	my ($self, $arg) = @_;
	my $aref = (ref $arg eq 'ARRAY' ? $arg : [$arg]);
	
	my $time = time();
	foreach my $elem (grep {$_} $self->get_idle(@$aref))
	{
		# we can't just decrease_key, the values get bigger as we go.
		$self->idle_heap->delete($elem);
		$elem->[2] = $time;
		$self->idle_heap->add($elem);
	}
}

sub BUILD 
{
	my $self = shift;
	POE::Session->create(
		object_states => [ $self => [qw(_start _expiration_check)] ],
	);
	$self->children({FRONT => $self->front, BACK => $self->back});
	$self->add_names qw(COMPLEX);
}

sub store
{
	my ($self, $message, $callback) = @_;
	my $id = $message->id;

	$self->more_front($message->size);
	$self->set_front($id => {persisted => 0, size => $message->size});
	$self->set_idle($id);

	# Move a bunch of messages to the backstore to keep size respectable
	my (@bump, %need_persist);
	while($self->front_size > $self->front_max)
	{
		my $top = $self->idle_heap->extract_top or last;
		my $id = $top->val;
		$need_persist{$id} = 1 unless $self->in_back($id);
		$self->less_front($self->delete_front($id)->{size});
		push(@bump, $id);
	}

	if(@bump)
	{
		my $idstr = join(', ', @bump);
		$self->log(info => "Bumping ($idstr) off the frontstore.");
		$self->delete_idle(@bump);
		$self->delete_expiration(@bump);
		$self->front->get(\@bump, sub {
			$self->front->remove(\@bump);
			$self->back->store($_) foreach 
				grep { $need_persist{$_->id} }    # things not in backstore
				grep { $_->persistent }           # that are persistent
				@{ $_[0] };                       # that we just got
		});
	}

	$self->set_expiration($id, time()) if ($message->persistent);
	$self->front->store($message, $callback);
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$poe_kernel->alias_set($self->alias);
	$poe_kernel->post($self->alias, '_expiration_check');
}

sub expire_messages
{
	my ($self, $ids) = @_;
	return unless @$ids;

	my $idstr = join(', ', @$ids);
	$self->log(info => "Pushing expired messages ($idstr) to backstore.");
	$_->{persisted} = 1 foreach $self->get_front(@$ids);
	$self->delete_expiration(@$ids);
	$self->front->get($ids, sub {
		# Messages in two places is dangerous, so we are careful!
		$self->back->store($_->clone) foreach (@{$_[0]});
	});
}

sub _expiration_check
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];

	return if $self->shutting_down;

	my $thresh = time() - $self->timeout;

	my $expired = [map  {$_->[0]} 
	               grep {my $s = $_->[1]; $s > 0 && $s < $thresh} 
	               $self->expiration_pairs];

	$self->expire_messages($expired);
	$kernel->alarm(_expiration_check => time() + $self->granularity);
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->shutting_down(1);

	# shutdown our check messages session
	$poe_kernel->alias_remove($self->alias);

	$self->front->get_all(sub {
		my $message_aref = $_[0];

		my @messages = grep {$_->persistent && !$self->in_back($_)}
		               @$message_aref;
		
		$self->log(info => 'Moving all messages into backstore.');
		$self->back->store($_) foreach @messages;
		$self->front->empty(sub {
			$self->front->storage_shutdown(sub {
				$self->back->storage_shutdown($complete);
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
