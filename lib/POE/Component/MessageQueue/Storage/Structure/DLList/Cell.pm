package POE::Component::MessageQueue::Storage::Structure::DLList::Cell;
use strict;
use warnings;

use constant NEXT  => 0;
use constant FIRST => 0;
use constant PREV  => 1;
use constant LAST  => 1;
use constant DATA  => 2;

use Symbol;

our $NIL;
BEGIN
{
	$NIL = gensym();
}

sub new
{
	my $self = bless [undef,undef,undef];
	$self->[FIRST] = $self;
	$self->[LAST] = $self;

	# This is just a random unique value used to identify the sentinel
	$self->[DATA] = $NIL;
	return $self;
}

sub _move
{
	my ($cell, $direction) = @_;
	my $next = $cell->[$direction];
	return $next unless ($next->[DATA] == $NIL);
	return undef;
}

sub _directions
{
	my $end = shift;
	my $inward  = ($end == FIRST) ? NEXT : PREV;
	my $outward = ($end == FIRST) ? PREV : NEXT;

	return ($inward, $outward);
}

sub _remove {
	my ($sentinel, $end) = @_;
	return undef if ($sentinel->[$end] == $sentinel);
	my ($inward, $outward) = _directions($end);
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
	my ($inward, $outward) = _directions($end);

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

sub next
{
	return shift->_move(NEXT);
}

sub prev
{
	return shift->_move(PREV);
}

sub DESTROY {
  my $self = shift;
  print "Destroying the cell with $self->[DATA] in it.\n";
}

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
