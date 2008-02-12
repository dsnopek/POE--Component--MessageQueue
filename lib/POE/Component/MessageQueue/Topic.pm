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

package POE::Component::MessageQueue::Topic;
use Moose;

sub destination { return '/topic/'.$_[0]->name };
sub is_persistent { return 0 }

with qw(POE::Component::MessageQueue::Place);

make_immutable;

sub send
{
	my ($self, $message) = @_;

	foreach my $subscriber (values %{$self->subscriptions})
	{
		$self->dispatch_message($message, $subscriber);
	}

	return;
}

1;

