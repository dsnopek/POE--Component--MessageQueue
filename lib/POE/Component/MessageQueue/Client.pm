
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

package POE::Component::MessageQueue::Client;
use Moose;

use POE::Component::MessageQueue::Subscription;
use POE::Kernel;

has 'subscriptions' => (
	is => 'ro',
	isa => 'HashRef',
	default => sub { {} },
);

has 'id' => (
	is       => 'ro',
	required => 1,
);

has 'connected' => (
	is => 'rw',
	default => 0,
);

has 'login' => (is => 'rw');
has 'passcode' => (is => 'rw');

make_immutable;

sub subscribe
{
	my ($self, $destination, $ack_type) = @_;

	my $subscription = POE::Component::MessageQueue::Subscription->new(
		destination => $destination,
		client      => $self,
		ack_type    => $ack_type,	
	);

	$self->subscriptions->{$destination->name} = $subscription;
	$destination->subscriptions->{$self->id} = $subscription;
	return $subscription;
}

sub unsubscribe
{
	my ($self, $destination) = @_;

	delete $self->subscriptions->{$destination->name};
	delete $destination->subscriptions->{$self->id};

	if ($destination->is_persistent)
	{
		$destination->storage->disown_destination($destination->name, $self->id);
	}
}

sub send_frame
{
	my ($self, $frame) = @_;
	my ($session, $socket);

	return unless ($session = $poe_kernel->alias_resolve($self->id));
	return unless ($socket = $session->get_heap()->{client});

	$socket->put($frame);
	return 1;
}

sub connect
{
	my ($self, $login, $passcode) = @_;

	$self->login($login);
	$self->passcode($passcode);
	$self->connected(1);

	my $id = $self->id;
	$self->send_frame(Net::Stomp::Frame->new({
		command => "CONNECTED",
		headers => {
			session => "client-$id",
		},
	}));
}

sub shutdown
{
	my $self = shift;

	$poe_kernel->post($self->id, "shutdown");
}

1;

