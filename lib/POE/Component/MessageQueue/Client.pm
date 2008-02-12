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

package POE::Component::MessageQueue::Client;

use POE::Kernel;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$client_id = $args->{client_id};
	}
	else
	{
		$client_id = $args;
	}

	my $self = 
	{
		client_id   => $client_id,
		# a list of queues we are subscribed to.
		queue_names => [ ],
		connected   => 0,
		login       => '',
		passcode    => '',
	};

	bless  $self, $class;
	return $self;
}

sub get_subscribed_queue_names
{
	my $self = shift;
	my @queues = @{ $self->{queue_names} || [] };
	return @queues;
}

sub _add_queue_name
{
	my ($self, $queue_name) = @_;
	push @{$self->{queue_names}}, $queue_name;
}

sub _remove_queue_name
{
	my ($self, $queue_name) = @_;

	my $i;
	my $max = scalar @{$self->{queue_names}};

	for( $i = 0; $i < $max; $i++ )
	{
		if ( $self->{queue_names}->[$i] == $queue_name )
		{
			splice @{$self->{queue_names}}, $i, 1;
			return;
		}
	}
}

sub send_frame
{
	my $self  = shift;
	my $frame = shift;

	my $client_session = $poe_kernel->alias_resolve( $self->{client_id} );

	# Check to see if the client's session is still around
	if ( defined $client_session )
	{
		my $client = $client_session->get_heap()->{client};

		# Check to see if the socket's Wheel is still around
		if ( defined $client )
		{
			$client->put($frame);

			return 1;
		}
	}

	return 0;
}

sub connect
{
	my $self = shift;
	my $args = shift;

	my $login;
	my $passcode;

	if ( ref($args) eq 'HASH' )
	{
		$login    = $args->{login};
		$passcode = $args->{passcode};
	}
	else
	{
		$login    = $args;
		$passcode = shift;
	}

	# set variables, yo!
	$self->{login}     = $login;
	$self->{passcode}  = $passcode;
	$self->{connected} = 1;

	# send connection confirmation
	my $response = Net::Stomp::Frame->new({
		command => "CONNECTED",
		headers => {
			session => "client-$self->{client_id}",
		},
	});
	$self->send_frame( $response );
}

sub shutdown
{
	my $self = shift;

	$poe_kernel->post( $self->{client_id}, "shutdown" );
}

1;

