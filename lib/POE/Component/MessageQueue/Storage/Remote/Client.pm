#
# Copyright 2008 Paul Driver <frodwith@gmail.com>
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

package POE::Component::MessageQueue::Storage::Remote::Client;
use Moose;
use Data::UUID;
use POE;
use POE::Component::Client::TCP;

has servers => (
	is       => 'ro',
	isa      => 'ArrayRef[HashRef]',
	required => 1,
);

has session_id => (
	is       => 'rw',
	isa      => 'Int',
	init_arg => undef,
);

has idmaker => (
  is       => 'ro',
	isa      => 'Data::UUID',
	init_arg => undef,
  default  => sub { Data::UUID->new() },
);

sub add_server_method
{
	my ($class, $method) = @_;
	$class->meta->add_method($method, sub {
		$poe_kernel->post($_[0]->session_id, remote_call => { 
			method => $method, 
			args   => [@_],
		});
	});
}

my %methods = %{
	POE::Component::MessageQueue::Storage->meta->get_required_method_map
};

delete $methods{storage_shutdown};

__PACKAGE__->add_server_method($_) foreach (keys %methods);

sub storage_shutdown
{
	my ($self, $callback) = @_;
	$poe_kernel->post($self->session_id, 'shutdown');
	goto $callback;
}

with qw(POE::Component::MessageQueue::Storage);

sub BUILD
{
	my ($self, $args) = @_;
	my ($host, $port) = ($self->host, $self->port);

	my @servers = $self->hosts;
	my $si = 1;
	my $total_fail = 0;
	my $retry = sub {
		if ($si >= @servers) 
		{
			if (++$total_fail > 2)
			{
				$self->log(emergency => 
					"Tried to connect to all servers $total_fail times without success."
				);
			}
			$si = 0;
		}
		my $info = $servers[$si++];
		$poe_kernel->delay(connect => 2 => $info->{addr} => $info->{port});
	};

	$self->session_id(POE::Component::Client::TCP->new(
		RemoteAddress  => $servers[0]->{addr},
		RemotePort     => $servers[0]->{port},
		ConnectTimeout => 3,
		Filter         => POE::Filter::Reference->new("YAML"),
		ObjectStates   => [ $self => ['remote_call'] ],

		Connected      => sub {
			my $heap = $_[HEAP];
			$heap->{server}->put($_) foreach (values %{ $heap->{calls} });
			$total_fail = 0;
		},

		ConnectError   => sub {
			$self->log(error => "Could not connect to $host:$port");
			goto $retry;
		},

		Disconnected   => $retry,

		ServerInput    => sub {
			my ($heap, $request) = @_[HEAP, ARG0];
			my $id = $request->{callback};
			my $args = $request->{args} || [];
			my $call = delete $heap->{calls}->{$id};
			if (my $code = $call->{callback})
			{
				$code->(@$args);
			}
		},

		ServerError    => sub {
			$self->log(error => "Remote error on ". $_[ARG0] . ": " . $_[ARG2]);
			goto $retry;
		},

	));
}

sub remote_call
{
	my ($self, $heap, $request) = @_[OBJECT, HEAP, ARG0];

	my $id = $self->idmaker->create_b64();
	my $call = $heap->{calls}->{$id} = { request => $request };

	my $args = $request->{args};
	my $last = $args->[-1];
	if (ref $last eq 'CODE')
	{
		$call->{callback} = $last;
		$args->[-1] = $id;
	}
	
	$heap->{server}->put($request) if $heap->{connected};
}

1;
