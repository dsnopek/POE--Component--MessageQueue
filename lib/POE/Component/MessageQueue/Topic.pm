#
# Copyright 2007 Paul Driver <frodwith@gmail.com>
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

package POE::Component::MessageQueue::Topic;

use strict;
use warnings;

sub new
{
	my ($class, $name, $notify) = @_;
	my $self = {
		name    => $name,
		clients => {},
		notify  => $notify,
	};
	bless  $self, $class;
	return $self;
}

sub add_subscription
{
	my ($self, $client) = @_;
	$self->{clients}->{$client->{client_id}} = $client;
	return;
}

sub remove_subscription
{
	my ($self, $client) = @_;
	delete $self->{clients}->{$client->{client_id}};
	return;
}

sub send_message
{
	my ($self, $message) = @_;

	foreach my $client (values %{$self->{clients}})
	{
		$self->{notify}->notify('dispatch', {
			topic   => $self,
			message => $message,
			client  => $client,
		});
		$client->send_frame($message->create_stomp_frame());
	}

	return;
}

1;

