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

package POE::Component::MessageQueue::Test::MQ;
use strict;
use warnings;
use Exporter qw(import);
our @EXPORT = qw(start_mq stop_mq);

sub start_mq {
	my $pid      = fork;
	my $storage  = shift || 'Memory';
	return $pid if $pid;
	
	use POE;
	use POE::Component::MessageQueue;
	use POE::Component::MessageQueue::Logger;
	use POE::Component::MessageQueue::Storage::Memory;
	use POE::Component::MessageQueue::Test::EngineMaker;

	# required for scripts which call start_mq() more than once.
	$poe_kernel->stop();

	my %defaults = (
		port    => 8099,
		storage => make_engine($storage),
		logger  => POE::Component::MessageQueue::Logger->new(level=>7),
	);

	my %options = @_;
	$defaults{$_} = $options{$_} foreach (keys %options);

	POE::Component::MessageQueue->new(%defaults);

	$poe_kernel->run();
	exit 0;
}

sub stop_mq {
	my $pid = shift;
	
	return (kill('TERM', $pid) == 1)
		&& (waitpid($pid, 0) == $pid)
		&& ($? == 0);
}

1;
