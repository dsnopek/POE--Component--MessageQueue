package POE::Component::MessageQueue::Storage::Structure::DLList;
use strict;
use warnings;

use constant NEXT => 0;
use constant PREV => 1;
use constant DATA => 2;

use Symbol;

our $NIL;
BEGIN
{
  $NIL = gensym();
}

sub new {
  my $self = bless [undef,undef,undef];
  $self->[NEXT] = $self;
  $self->[PREV] = $self;
  $self->[DATA] = $NIL;
  return $self;
}

sub enqueue {
  my ($self, $data) = @_; 
  my $cell = bless [
    $self->[NEXT], 
    $self, 
    $data,
  ];
  $self->[NEXT]->[PREV] = $cell;
  $self->[NEXT] = $cell;
  return $cell;
}

sub dequeue {
  my $self = shift;
  return undef if ($self->[PREV] == $self);

  my $item = $self->[PREV];
  my $tail = $item->[PREV];

  $self->[PREV] = $tail; 
  $tail->[NEXT] = $self;

  return $item;
}

# Don't call these on the main object, but rather on the saved values of
# enqueue (items in the list).

sub delete {
  my $self = shift;
  my $next = $self->[NEXT];
  my $prev = $self->[PREV];
  $next->[PREV] = $prev;
  $prev->[NEXT] = $next;
  return $self;
}

sub data {
  return shift->[DATA];
}

sub next {
  my $next = shift->[NEXT];
  return $next unless ($next->[DATA] == $NIL);
}

sub prev {
  my $prev = shift->[PREV];
  return $prev unless ($prev->[DATA] == $NIL);
}

1;
