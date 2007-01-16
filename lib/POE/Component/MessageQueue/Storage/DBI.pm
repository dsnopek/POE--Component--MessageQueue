
package POE::Component::MessageQueue::Storage::DBI;

use POE::Kernel;
use POE::Session;
use POE::Component::EasyDBI;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $dsn;
	my $username;
	my $password;
	my $options;

	if ( ref($args) eq 'HASH' )
	{
		$dsn      = $args->{dsn};
		$username = $args->{username};
		$password = $args->{password};
		$options  = $args->{options};
	}

	my $self = {
		message_id        => 0,
		claiming          => { },
		dispatch_message  => undef,
		destination_ready => undef,
	};
	bless $self, $class;

	my $easydbi = POE::Component::EasyDBI->spawn(
		alias    => 'MQ-DBI',
		dsn      => $dsn,
		username => $username,
		password => $password
	);

	my $session = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->alias_set('MQ-Storage')
			},
		},
		object_states => [
			$self => [
				'_init_message_id',
				'_easydbi_handler',
				'_message_from_store',
				'_store_claim_message'
			]
		]
	);

	# store the sessions
	$self->{easydbi} = $easydbi;
	$self->{session} = $session;

	# clear the state
	$poe_kernel->post( $self->{easydbi},
		do => {
			sql     => 'UPDATE messages SET in_use_by = NULL',
			event   => '_easydbi_handler',
			session => $self->{session}
		}
	);

	# get the initial message id
	$poe_kernel->post( $self->{easydbi},
		single => {
			sql     => 'SELECT MAX(message_id) FROM messages',
			event   => '_init_message_id',
			session => $self->{session}
		}
	);

	return $self;
}

sub set_dispatch_message_handler
{
	my ($self, $handler) = @_;
	
	$self->{dispatch_message} = $handler;
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;

	$self->{destination_ready} = $handler;
}

sub get_next_message_id
{
	my $self = shift;
	return ++$self->{message_id};
}

sub store
{
	my ($self, $message) = @_;

	# push the message into our persistent store
	$poe_kernel->post( $self->{easydbi},
		insert => {
			table   => 'messages',
			hash    => { %$message },
			session => $self->{session},
			event   => '_easydbi_handler',

			# baggage:
			_message_id => $message->{message_id}
		}
	);
}

sub remove
{
	my ($self, $message_id) = @_;

	# remove the message from the backing store
	$poe_kernel->post( $self->{easydbi},
		do => {
			sql          => 'DELETE FROM messages WHERE message_id = ?',
			placeholders => [ $message_id ],
			session      => $self->{session},
			event        => '_easydbi_handler'
		}
	);
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	my $destination;
	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
		$client_id   = $args->{client_id};
	}
	else
	{
		$destination = $args;
		$client_id   = shift;
	}

	if ( $self->{claiming}->{$destination} )
	{
		# we are already attempting to claim a message for this destination!
		return 0;
	}
	else
	{
		# lock temporarily.
		$self->{claiming}->{$destination} = $client_id;
	}

	$poe_kernel->post( $self->{easydbi},
		arrayhash => {
			sql          => 'SELECT * FROM messages WHERE destination = ? AND in_use_by IS NULL ORDER BY message_id ASC LIMIT 1',
			placeholders => [ $destination ],
			session      => $self->{session},
			event        => '_message_from_store',

			# baggage:
			_destination => $destination,
			_client_id   => $client_id
		}
	);

	# let the caller know that this is actually going down.
	return 1;
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $client_id) = @_;

	$poe_kernel->post( $self->{easydbi},
		do => {
			sql          => 'UPDATE messages SET in_use_by = NULL WHERE in_use_by = ?',
			placeholders => [ $client_id ],
			session      => $self->{session},
			event        => 'easydbi_handler',
		}
	);
}

sub _init_message_id
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	$self->{message_id} = $value->{result} || 0;
}

sub _easydbi_handler
{
	my ($self, $kernel, $event) = @_[ OBJECT, KERNEL, ARG0 ];

	if ( $event->{action} eq 'insert' )
	{
		print "STORE: DBI: Added message $event->{_message_id} to backing store\n";
	}
	elsif ( $event->{action} eq 'do' )
	{
		my $pretty = join ', ', @{$event->{placeholders}};
		print "STORE: DBI: $event->{sql} [ $pretty ]\n";
	}
}

sub _message_from_store
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	my $rows        = $value->{result};
	my $destination = $value->{_destination};
	my $client_id   = $value->{_client_id};

	if ( not defined $self->{dispatch_message} )
	{
		die "Pulled message from backstore, but there is no dispatch_message handler";
	}

	my $message;

	if ( defined $rows and scalar @$rows == 1 )
	{
		my $result = $rows->[0];

		$message = POE::Component::MessageQueue::Message->new({
			message_id  => $result->{message_id},
			destination => $result->{destination},
			persistent  => $result->{persistent},
			body        => $result->{body},
			in_use_by   => $client_id
		});

		# claim this message
		$kernel->post( $self->{easydbi},
			do => {
				sql          => "UPDATE messages SET in_use_by = ? WHERE message_id = ?",
				placeholders => [ $client_id, $message->{message_id} ],
				session      => $self->{session},
				event        => '_store_claim_message',

				# backage:
				_destination => $destination,
				_client_id   => $client_id,
			}
		);

	}
	elsif ( scalar @$rows > 1 )
	{
		die "ERROR!  Somehow two messages got attached to the same client!\n";
	}

	# sneak the message into the arguments
	splice(@_, ARG0, 1, $message);

	# call the handler
	$self->{dispatch_message}->( @_ );
}

sub _store_claim_message
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	my $result      = $value->{result};
	my $destination = $value->{_destination};
	my $client_id   = $value->{_client_id};

	# unlock claiming from this destination
	delete $self->{claiming}->{$destination};

	# notify whoaver, that the destination is ready for another client to try to claim
	# a message.
	if ( defined $self->{destination_ready} )
	{
		print "destination: $destination\n";
		splice(@_, ARG0, 1, $destination);

		$self->{destination_ready}->(@_);
	}
}

1;

