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

use strict;
use warnings;
package POE::Component::MessageQueue::Message::ID::Generator::UUID;
use base qw(POE::Component::MessageQueue::Message::ID::Generator);

use Data::UUID;
use POE::Component::MessageQueue::Message::ID::UUID;

sub new 
{
	my $class = shift;
	my $self = $class->SUPER::new(@_);

	$self->{gen} = Data::UUID->new();

	return bless $self, $class;
}

sub generate 
{
	my ($self, $message) = @_;
	my $uuid = $self->{gen}->create();
	return POE::Component::MessageQueue::Message::ID::UUID->new($uuid);
}

1;

=head1 NAME

POE::Component::MessageQueue::Message::ID::Generator::UUID - UUID generator.

=head1 DESCRIPTION

This is a concrete implementation of the Generator interface for creating
message IDs.  It uses standards compliant UUIDs.  These aren't guaranteed to
be unique, but they extremely unlikely to collide.  To paraphrase Wikipedia,
if we generated one UUID every second for the next 100 years, the probability
of having one collision is about 50%.

=head1 SEE ALSO

L<Data::UUID>

=head1 AUTHOR

Paul Driver <frodwith@gmail.com>
