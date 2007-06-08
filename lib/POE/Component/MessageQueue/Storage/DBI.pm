package POE::Component::MessageQueue::Storage::DBI;
use base qw(POE::Component::MessageQueue::Storage::Generic);

use POE::Component::MessageQueue::Storage::Generic::DBI;
use strict;

sub new
{
        my $class = shift;

        my $self = $class->SUPER::new({
                package      => 'POE::Component::MessageQueue::Storage::Generic::DBI',
                options      => \@_,
        });

        bless  $self, $class;
        return $self;
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::DBI -- A storage engine that uses L<DBI>

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::DBI;
  use strict;

  # For mysql:
  my $DB_DSN      = 'DBI:mysql:database=perl_mq';
  my $DB_USERNAME = 'perl_mq';
  my $DB_PASSWORD = 'perl_mq';
  my $DB_OPTIONS  = undef;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::DBI->new({
      dsn      => $DB_DSN,
      username => $DB_USERNAME,
      password => $DB_PASSWORD,
      options  => $DB_OPTIONS
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage engine that uses L<DBI>.  All messages stored with this backend are
persistent.

Performance is increased greatly by wrapping this engine in 
L<POE::Component::MessageQueue::Storage::Throttled> at the expense of being slower
to persist messages.

This module is really just L<POE::Component::MessageQueue::Storage::Generic> with
L<POE::Component::MessageQueue::Storage::Generic::DBI>.  See the documentation for
those modules for more information (primarily
L<POE::Component::MessageQueue::Storage::Generic::DBI>).

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item dsn => SCALAR

=item username => SCALAR

=item password => SCALAR

=item options => SCALAR

=back

=head1 SEE ALSO

L<DBI>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

