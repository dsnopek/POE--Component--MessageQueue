#
# Copyright 2007, 2008 Paul Driver <frodwith@gmail.com>
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

package POE::Component::MessageQueue::Storage::Structure::DLList;
use strict;
use warnings;

use POE::Component::MessageQueue::Storage::Structure::DLList::Cell;
use constant FIRST =>
	POE::Component::MessageQueue::Storage::Structure::DLList::Cell::FIRST;
use constant LAST =>
	POE::Component::MessageQueue::Storage::Structure::DLList::Cell::LAST;

sub new
{
	my $class = shift;
	my $sentinel =
		POE::Component::MessageQueue::Storage::Structure::DLList::Cell->new();

	# Self will just be a blessed reference to a sentinel node.
	my $self = \$sentinel;
	return bless $self, $class;

	return $self;
}

sub shift : method
{
	my $self = shift;
	$$self->_remove(FIRST);
}

sub pop : method
{
	my $self = shift;
	$$self->_remove(LAST);
}

sub unshift : method
{
	my ($self, $data) = @_;
	return $$self->_add(FIRST, $data);
}

sub push : method
{
	my ($self, $data) = @_;
	return $$self->_add(LAST, $data);
}

sub enqueue
{
	my ($self, $data) = @_;
	return $self->push($data);
}

sub dequeue
{
	return shift->shift();
}

sub first
{
	my $self = shift;
	return $$self->next();
}

sub last
{
	my $self = shift;
	return $$self->prev();
}

sub DESTROY 
{
	my $self = shift;
	my $sentinel = $$self;

	# The cells already have weakened prev pointers, so as soon as we break the
	# circularity everything will be fine.
	$sentinel->_break();
}

1;

=head1 NAME

POE::Component::MessageQueue::Storage::Structure::DLList - 
A simple doubly-linked list for wheel-reinventing goodness.

=head1 SYNOPSIS

	use POE::Component::MessageQueue::Storage::Structure::DLList;

	my $list = POE::Component::MessageQueue::Storage::Structure::DLList->new();
	$list->push(1);
	$list->push(2);
	$list->pop(); # --> 2;

	$list->unshift($_) for (1..20);

	# List cells know how to delete themselves
	$list->next()->next()->delete();

=head1 DESCRIPTION

Just a doubly linked list with the common list methods (push, pop, etc.).  The
main point of using this module instead of standard perl arrays is that cells
know how to delete themselves from the list.  Thich lets us do some tricky
things like having a hash that points to queue cells and doing deletion
from lists by some arbitrary hash index in O(1).

Individual cells in the list are instances of
L<POE::Component::MessageQueue::Storage::Structure::DLList::Cell>

=head1 METHODS

These are intended to be called on the return value of new(), and deal with
operations on the whole list (rather than individual cells in it).

=over 4

=item new

Returns a DLList with nothing in it.  This is what you call list methods on.

=item shift

=item unshift

=item push

=item pop

These work just like the builtins.  Pop and shift return you the data that was
in the (now useless) list cell.  Push and unshift return you the list cell.

=item queue

=item dequeue

These work just like push/shift.  In fact, they probably are push/shift.

=item first

=item last

The first and last cells in the list, respectively.

=back

=head1 BUGS AND LIMITATIONS

This is fairly slow for most things you'd use a list for - much slower than 
vanilla perl arrays.  Only use this if you know what you're doing and need the 
advantages it offers!

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::Structure::DLList::Cell>

=head1 AUTHOR

Paul Driver <frodwith@gmail.com>
