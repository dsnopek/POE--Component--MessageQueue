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

package POE::Component::MessageQueue::Storage::Structure::DLList::Cell;
use strict;
use warnings;

use Symbol;
use Scalar::Util qw(weaken);

use constant NEXT  => 0;
use constant FIRST => 0;
use constant PREV  => 1;
use constant LAST  => 1;
use constant DATA  => 2;

use constant NIL => gensym();

use constant DIRECTIONS => (
	# Inward, outward
	[NEXT, PREV], # end = FIRST
	[PREV, NEXT], # end = LAST 
);

# We solve our circular reference problems by always weakening our prev
# pointers and having whoever has reference to our sentinel node call _break()
# on it when they're done with us (usually in a DESTROY).

sub _break {
	my $self = shift;
	$self->[FIRST] = undef;
	$self->[LAST] = undef;
}

sub new
{
	my $self = bless [undef,undef,undef];

	$self->[FIRST] = $self;
	$self->[LAST] = $self;

	# This is just a random unique value used to identify the sentinel
	$self->[DATA] = NIL;
	return $self;
}

# End means "which end are we stuffing this on".  Can be FIRST or LAST.
sub _remove {
	my ($sentinel, $end) = @_;
	return undef if ($sentinel->[$end] == $sentinel);
	my ($inward, $outward) = @{(DIRECTIONS)[$end]};
	my $old_end = $sentinel->[$end];
	my $new_end = $old_end->[$inward];

	$sentinel->[$end] = $new_end;
	$new_end->[$outward] = $sentinel;

	weaken($new_end->[PREV]);
	return $old_end->[DATA];
}

sub _add
{
	my ($sentinel, $end, $data) = @_;
	my ($inward, $outward) = @{(DIRECTIONS)[$end]};

	my $new_cell = bless [undef, undef, $data];
	$new_cell->[$inward] = $sentinel->[$end];
	$new_cell->[$outward] = $sentinel;

	$sentinel->[$end]->[$outward] = $new_cell;
	$sentinel->[$end] = $new_cell;

	weaken($new_cell->[PREV]);
	return $new_cell; 
}

sub delete
{
	my $cell = shift;
	my $next = $cell->[NEXT];
	my $prev = $cell->[PREV];
	$next->[PREV] = $prev;
	$prev->[NEXT] = $next;

	weaken($next->[PREV]);
	return $cell->[DATA];
}

sub data
{
	return shift->[DATA];
}

sub _move
{
	my ($cell, $direction) = @_;
	my $next = $cell->[$direction];
	return $next unless ($next->[DATA] == NIL);
	return undef;
}

sub next {_move(@_, NEXT) }
sub prev {_move(@_, PREV) }

1;

=head1 NAME

POE::Component::MessageQueue::Storage::Structure::DLList::Cell - 
Cells in a doubly linked list.

=head1 DESCRIPTION

These are just cells in a doubly linked list.  The cool thing is that they
know how to delete themselves and handle being circularly referenced like
champions.

=head1 METHODS

=item next

=item prev

=item data

Should be obvious, right?

=item delete

Deletes this item from the list and returns the data that was stored in it.

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::Structure::DLList>

=head1 AUTHOR

Paul Driver <frodwith@gmail.com>
