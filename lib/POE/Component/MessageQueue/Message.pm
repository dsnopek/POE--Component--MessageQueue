#
# Copyright 2007, 2008 David Snopek <dsnopek@gmail.com>
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

package POE::Component::MessageQueue::Message;
use Moose;
use Net::Stomp::Frame;

# Use Time::HiRes's time() if available.  Without it, messages that are sent
# rapidly may come out in an odd order.
BEGIN { eval { require Time::HiRes; Time::HiRes->import qw(time); } };

has 'id' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,
);

has 'destination' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,
);

has 'body' => (
	is => 'rw',
	clearer => 'delete_body',
);

has 'persistent' => (
	is       => 'ro',
	isa      => 'Bool',
	required => 1,
);

has 'claimant' => (
	is        => 'rw',
	isa       => 'Maybe[Int]',
	writer    => 'claim',
	predicate => 'claimed',
	clearer   => 'disown',
);

has 'size' => (
	is      => 'ro',
	isa     => 'Int',
	lazy    => 1,
	default => sub {length $_[0]->body},
);

has 'timestamp' => (
	is      => 'ro',
	isa     => 'Num',
	default => sub { time() },
);

make_immutable;

sub equals
{
	my ($self, $other) = @_;
	foreach my $ameta (values %{__PACKAGE__->meta->get_attribute_map})
	{
		my $reader = $ameta->get_read_method;
		my ($one, $two) = ($self->$reader, $other->$reader);
		next if (!defined $one) && (!defined $two);
		return 0 unless (defined $one) && (defined $two);
		return 0 unless ($one eq $two);
	}
	return 1;
}

sub create_stomp_frame
{
	my $self = shift;

	return Net::Stomp::Frame->new({
		command => 'MESSAGE',
		headers => {
			'destination'    => $self->destination,
			'message-id'     => $self->id,
			'content-length' => $self->size,
		},
		body => $self->body,
	});
}

1;

