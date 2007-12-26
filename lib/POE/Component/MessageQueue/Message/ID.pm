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
package POE::Component::MessageQueue::Message::ID;

use overload ('""' => 'as_string');

sub new 
{
	my ($class, $val) = @_; 

	return bless({
		value => $val,
	}, $class);
}

sub raw
{
	my $self = shift;
	return $self->{value};
}

sub as_string
{
	my $self = shift;
	return "$self->{value}";
}

sub from_string
{
	my ($class, $string) = @_;
	die "Abstract.";
}

1;

=head1 NAME

POE::Component::MessageQueue::ID - Abstract base class for message IDs.

=head1 DESCRIPTION

This just stores any value it's sent in new and returns it for raw and a
forced string conversion for as_string. You can't use this directly (you need
to at least override from_string), it's just here to provide an interface.

=head1 CLASS METHODS

=over 4

=item from_string

Should construct a new instance from a string representation (whatever
as_string would spit out).  $class->from_string($obj->as_string) should be the
same as $obj.

=back

=head1 METHODS

=over 4

=item new => SCALAR

The argument should be whatever the "raw" value for the ID is supposed to be
(or enough information to construct that).

=item raw

Sort of explanatory, whatever the "true" value of this object is.

=item as_string

Should return some SANE string representation of the object (something that
would make sense as a hash key).  Whenever perl tries to stringify the object,
this is called.

=back

=head1 AUTHOR

Paul Driver <frodwith@gmail.com>
