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

use strict;
use warnings;
package POE::Component::MessageQueue::IDGenerator::SimpleInt;
use base qw(POE::Component::MessageQueue::IDGenerator);

sub new 
{
	my ($class, $filename, @rest) = @_;
	my $self = $class->SUPER::new(@rest);
	
	die "No filename for last_id storage." unless $filename;
	$self->{filename} = $filename;

	if (-e $filename) 
	{
		open my $in, '<', $filename || 
			die "Couldn't open $filename for reading: $!";	
		my $line = <$in>;
		close $in;
		chomp $line;
		die "$filename didn't contain a number." unless ($line =~ /^\d+$/);
		$self->{last_id} = 0 + $line;
	}
	else
	{
		open my $out, '>', $filename ||
			die "Couldn't touch $filename: $!";
		close $out;
		$self->{last_id} = 0;
	}

	return bless $self, $class;
}

sub generate 
{
	my ($self, $message) = @_;
	my $id = ++$self->{last_id};
	return "$id";
}

sub DESTROY 
{
	my $self = shift;
	open my $out, '>', $self->{filename} ||
		die "Couldn't reopen $self->{filename} to write last ID!";
	print $out "$self->{last_id}\n";
	close $out;
}

1;

=head1 NAME

POE::Component::MessageQueue::IDGenerator::SimpleInt - Simple integer IDs.

=head1 DESCRIPTION

This is a concrete implementation of the Generator interface for creating
message IDs.  It simply increments an integer, and makes some attempt to
remember what the last one it used was across runs. 

=head1 AUTHOR

Paul Driver <frodwith@gmail.com>
