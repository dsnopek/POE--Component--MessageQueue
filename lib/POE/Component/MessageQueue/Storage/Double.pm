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

sub _get_many
{
	my ($front, $back, $callback) = @_;
	my %messages; # store in a hash to ensure uniqueness
	$front->(sub {
		$messages->{$_->id} = $_ foreach @{$_[0]};
		$back->(sub {
			$messages->{$_->id} = $_ foreach @{$_[0]};
			@_ = ([values %messages]);
			goto $callback;	
		}
	});
}

sub get_all
{
	my ($self, $callback) = @_;
	_get_many(
		sub {$self->front->get_all($_[0])},
		sub {$self->back ->get_all($_[0])},
		$callback,
	);
}

sub get_by_client
{
	my ($self, $client_id, $callback);
	_get_many(
		sub {$self->front->get_by_client($client_id, $_[0])},
		sub {$self->back ->get_by_client($client_id, $_[0])},
		$callback,
	);
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

sub claim_next
{
	my ($self, $destination, $client_id, $callback);
	$self->front->claim_next(sub {
		if (my $got = $_[0])
		{
			$self->back->claim($got->id, $client_id, sub { 
				@_ = ($got);
				goto $callback;
			});
		}
		else
		{
			$self->back->claim_next($destination, $client_id, sub {
				@_ = ($_[0]);
				goto $callback;
			});
		}
	});
}

sub _do_both
{
	my ($front, $back, $callback) = @_;
	if ($callback)
	{
		$front->(sub {
			$back->(sub {
				goto $callback;
			});
		});	
	}
	else
	{
		$front->();
		$back->();
	}
}

sub remove
{
	my ($self, $ids, $cb) = @_;
	_do_both(
		sub {$self->front->remove($ids, $_[0]),
		sub {$self->back ->remove($ids, $_[0]),
		$cb
	);
}

sub empty
{
	my ($self, $cb) = @_;
	_do_both(
		sub {$self->front->empty($_[0]),
		sub {$self->back ->empty($_[0]),
		$cb
	);
}

sub claim
{
	my ($self, $ids, $client_id, $cb) = @_;
	_do_both(
		sub {$self->front->claim($ids, $_[0])},
		sub {$self->back ->claim($ids, $_[0])},
		$cb,
	);
}

sub disown
{
	my ($self, $ids, $cb) = @_;
	_do_both(
		sub {$self->front->disown($ids, $_[0])},
		sub {$self->back ->disown($ids, $_[0])},
		$cb,
	);
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
