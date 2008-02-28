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

package POE::Component::MessageQueue::Storage::Double;
use Moose::Role;
with qw(POE::Component::MessageQueue::Storage);
use POE::Component::MessageQueue::Storage::BigMemory;

has 'front' => (
	is       => 'ro',
	does     => qw(POE::Component::MessageQueue::Storage),
	default  => sub {POE::Component::MessageQueue::Storage::BigMemory->new()},
	required => 1,
);

has 'back' => (
	is       => 'ro',
	does     => qw(POE::Component::MessageQueue::Storage),
	required => 1,
);

after 'set_logger' => sub {
	my ($self, $logger) = @_;
	$self->front->set_logger($logger);
	$self->back->set_logger($logger);
};

sub get
{
	my ($self, $ids, $callback) = @_;
	my %to_get = map {$_ => 1} @$ids;
	$self->front->get($ids, sub {
		my $got = $_[0];
		delete $to_get{$_->id} foreach (@$got);
		my @back_ids = keys %to_get;
		if(@back_ids > 0)
		{
			$self->back->get(\@back_ids, sub {
				push(@$got, @{$_[0]});
				@_ = ($got);
				goto $callback;
			});
		}
		else # Avoided a backstore call.  Efficiency!
		{
			@_ = ($got);
			goto $callback; 
		}
	});
}

sub get_all
{
	my ($self, $callback) = @_;
	my %messages; # store in a hash to ensure uniqueness
	$self->front->get_all->(sub {
		$messages->{$_->id} = $_ foreach @{$_[0]};
		$self->back->get_all->(sub {
			$messages->{$_->id} = $_ foreach @{$_[0]};
			@_ = ([values %messages]);
			goto $callback;	
		}
	});
}

sub get_oldest
{
	my ($self, $callback);
	$self->front->get_oldest(sub {
		my $f = $_[0];
		$self->back->get_oldest(sub {
			my $b = $_[0];
			@_ = (
				($f && $b) ? 
				($f->timestamp < $b->timestamp ? $f : $b) :
				($f || $b)
			);
			goto $callback;
		});
	});
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $callback);
	$self->front->claim_and_retrieve(sub {
		if (my $got = $_[0])
		{
			$self->back->claim($got->id, $client_id, sub { 
				@_ = ($got);
				goto $callback;
			});
		}
		else
		{
			$self->back->claim_and_retrieve($destination, $client_id, $callback);
		}
	});
}

foreach my $method qw(remove empty claim disown_destination disown_all)
{
	__PACKAGE__->meta->add_method($name, sub {
		my $self = shift;
		my $last = pop;
		if(ref $last eq 'CODE')
		{
			$self->front->$method(@args, sub {
				$self->back->$method(@args, $last);
			});
		}
		else
		{
			$self->front->$method(@_, $last);
			$self->back->$method(@_, $last);
		}
	});
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Double -- Stores composed of two other
stores.
 
=head1 DESCRIPTION

Refactor mercilessly, as they say.  They also say don't repeat yourself.  This
module contains functionality for any store that is a composition of two 
stores.  At least Throttled and Complex share this trait, and it doesn't make 
any sense to duplicate code between them.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item front => SCALAR

=item back => SCALAR

Takes a reference to a storage engine to use as the front store / back store.

=back

=head1 Unimplemented Methods

=over 2

=item store

This isn't implemented because Complex and Throttled differ here.  Perhaps
your storage differs here as well.  This is essentially where you specify
policy about what goes in which store.

=item storage_shutdown

And this is where you specify policy about what happens when you die.  You
lucky person, you.

=back

=cut
