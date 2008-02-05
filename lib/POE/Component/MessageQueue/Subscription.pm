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

package POE::Component::MessageQueue::Subscription;

use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $client;
	my $ack_type;

	if ( ref($args) eq 'HASH' )
	{
		$client   = $args->{client};
		$ack_type = $args->{ack_type};
	}
	else
	{
		$client   = $args;
		$ack_type = shift;
	}

	my $self =
	{
		client    => $client,
		ack_type  => $ack_type,
		ready     => 1
	};

	bless  $self, $class;
	return $self;
}

sub get_client            { return shift->{client}; }
sub get_ack_type          { return shift->{ack_type}; }
sub is_ready              { return shift->{ready}; }
sub set_handling_message  { shift->{ready} = 0; }
sub set_done_with_message { shift->{ready} = 1; }

1;

