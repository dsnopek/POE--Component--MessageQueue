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

# For these remove functions, the control flow is the same, but the
# particulars vary.
# front, back: call remove on their respective stores.
# combine: a function that aggregates the results of front and back.
# callback: the thing to callback with the results of the remove op.
sub _remove_underneath
{
	my %a = @_;

	# There are no results, so just call everything in parallel.
	unless ($a{callback}) 
	{
		$a{front}->();
		$a{back}->();
		return;
	}

	# Save the results of front and back, aggregate them, and pass them on.
	$a{front}->(sub {
		my $fresult = shift;
		$a{back}->(sub {
			my $bresult = shift;	
			$a{callback}->($a{combine}->($fresult, $bresult));
		});
	});
	return;
}

sub remove
{
	my ($self, $id, $cb) = @_;
	_remove_underneath(
		front    => sub { $self->front->remove($id, shift) },
		back     => sub { $self->back ->remove($id, shift) },
		combine  => sub { $_[0] || $_[1] },
		callback => $cb,
	);
	return;
}

# Combine for _multiple and _all
sub __append
{
	my ($one, $two) = @_;
	push(@$one, @$two);
	use YAML;
	print STDERR Dump($one);
	return $one;
}

# We'll call remove_multiple on the full range of ids - well-behaved stores
# will just ignore IDs they don't have.
sub remove_multiple
{
	my ($self, $ids, $cb);
	_remove_underneath(
		front    => sub { $self->front->remove_multiple($ids, shift) },
		back     => sub { $self->back ->remove_multiple($ids, shift) },
		combine  => \&__append,
		callback => $cb,
	);
	return;
}

sub remove_all
{
	my ($self, $cb);
	_remove_underneath(
		front    => sub { $self->front->remove_all(shift) },
		back     => sub { $self->back ->remove_all(shift) },
		combine  => \&__append,
		callback => $cb,
	);	
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;

	$self->front->claim_and_retrieve($destination, $client_id, sub {
		my $message = shift;
		if ($message)
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
	my ($self, @args) = @_;

	$self->front->disown(@args);
	$self->back->disown(@args);
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
