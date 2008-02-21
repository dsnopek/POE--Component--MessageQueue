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

sub peek_oldest
{
	my ($self, $callback) = @_;
	$self->front->peek_oldest(sub {
		my $front = $_[0];
		$self->back->peek_oldest(sub {
			my $back = $_[0];
			return ($front->timestamp < $back->timestamp) ? $front : $back;
		});
	});
}

sub peek
{
	my ($self, $ids, $callback) = @_;
	$self->front->peek($ids, sub {
		my $messages = $_[0];
		$self->back->peek($ids, sub {
			push(@$messages, @{$_[0]});
			$callback->($messages);
		});
	});
}

sub _remove_underneath
{
	my ($front, $back, $cb) = @_;
	if ($cb)
	{
		$front->(sub {
			my $fronts = $_[0];
			$back->(sub {
				my $backs = $_[0];

				# We can have messages that exist in both front and back stores, which
				# is allowed.  They're not allowed to be different though, so we can
				# throw duplicates away.
				my %uniques;
				$uniques{$_->id} = $_ foreach (@$backs);
				$uniques{$_->id} = $_ foreach (@$fronts);
				my @messages = values %uniques;

				$cb->(\@messages);
			});
		});
	}
	else
	{
		$front->();
		$back->();
	}
	return;
}

# We'll call remove on the full range of ids - well-behaved stores
# will just ignore IDs they don't have.
sub remove
{
	my ($self, $ids, $cb) = @_;
	_remove_underneath(
		sub { $self->front->remove($ids, shift) },
		sub { $self->back ->remove($ids, shift) },
		$cb
	);
	return;
}

sub empty
{
	my ($self, $cb) = @_;
	_remove_underneath(
		sub { $self->front->empty(shift) },
		sub { $self->back ->empty(shift) },
		$cb
	);
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;

	$self->front->claim_and_retrieve($destination, $client_id, sub {
		if (my $message = $_[0])
		{
			$dispatch->($message, $destination, $client_id);
		}
		else
		{
			$self->back->claim_and_retrieve(
				$destination, $client_id, $dispatch);
		}
	});
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id, $callback) = @_;

	if ($callback) 
	{
		$self->front->disown($destination, $client_id, sub {
			$self->back->disown($destination, $client_id, $callback);
		});
	}
	else
	{
		$self->front->disown($destination, $client_id);
		$self->back->disown($destination, $client_id);
	}
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Double -- Stores composed of two other
stores.
 
=head1 DESCRIPTION

Refactor mercilessly, as they say.  They also say don't repeat yourself.  This
module contains the functionality of any store that is a composition of two 
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
