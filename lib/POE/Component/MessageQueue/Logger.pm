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

package POE::Component::MessageQueue::Logger;

use POE::Kernel;
use strict;

my $LEVELS = {
	debug     => 0,
	info      => 1,
	notice    => 2,
	warning   => 3,
	error     => 4,
	critical  => 5,
	alert     => 6,
	emergency => 7
};

our $LEVEL = 3;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $logger_alias;
	my $log_function;

	if ( ref($args) eq 'HASH' )
	{
		$logger_alias = $args->{logger_alias};
		$log_function = $args->{log_function};
	}

	my $self = {
		logger_alias => $logger_alias,
		log_function => $log_function,
	};

	bless  $self, $class;
	return $self;
}

sub set_log_function
{
	my ($self, $func) = @_;
	$self->{log_function} = $func;
	undef;
}

sub set_logger_alias
{
	my ($self, $alias) = @_;
	$self->{logger_alias} = $alias;
	undef;
}

sub log
{
	my $self = shift;
	my $type = shift;
	my $msg  = shift;

	if ( not defined $msg )
	{
		$msg  = $type;
		$type = 'info';
	}

	if ( defined $self->{log_function} )
	{
		$self->{log_function}->( $type, $msg );
	}
	elsif ( defined $self->{logger_alias} )
	{
		$poe_kernel->post( $self->{logger_alias}, $type, "$msg\n" );
	}
	elsif ( $LEVELS->{$type} >= $LEVEL )
	{
		print STDERR "$msg\n";
	}
}

sub shutdown
{
	my $self = shift;

	if ( defined $self->{logger_alias} )
	{
		$poe_kernel->signal( $self->{logger_alias}, 'TERM' );
	}
}

1;

