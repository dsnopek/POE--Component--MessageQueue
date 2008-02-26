use warnings;
use Test::More qw(no_plan);
use File::Path;
use POE;
use POE::Session;
use YAML; # for Dump!

use constant MQ_PREFIX => 'POE::Component::MessageQueue';
use constant DATA_DIR  => '/tmp/mq_test';
use constant DB_FILE   => DATA_DIR.'/mq.db';
use constant DSN       => 'DBI:SQLite:dbname='.DB_FILE;

my %engines;
BEGIN {
	sub engine_package {MQ_PREFIX.'::Storage::'.shift} 
	%engines = (
		DBI        => {
			args    => sub {(
				dsn      => DSN,
				username => q(),
				password => q(),
			)},
		},
		FileSystem => {
			args     => sub {(
				info_store => make_engine('DBI'),
				data_dir   => DATA_DIR,
			)},
		},
		Throttled  => {
			args    => sub {(
				throttle_max => 2,
				back         => make_engine('FileSystem'),
			)},
		},
		Complex    => {
			args    => sub {(
				timeout     => 4,
				granularity => 2,
				front_max   => 1024,
				front       => make_engine('BigMemory'),
				back        => make_engine('Memory'),
#				back        => make_engine('Throttled'),
			)}
		},
		BigMemory => {},
		Memory    => {},
	);
}
BEGIN {
	require_ok(MQ_PREFIX.'::Message');
	require_ok($_) foreach map { engine_package($_) } (keys %engines);
	require_ok(MQ_PREFIX.'::Storage::Default');
}
END {
	rmtree(DATA_DIR);	
}

sub make_engine {
	my $name = shift;
	my $e = $engines{$name};
	my $args = $e->{args} || sub {};
	engine_package($name)->new($args->());
}

my $next_id = 0;
my $when = time();
my @destinations = map {"/queue/$_"} qw(foo bar baz grapefruit);
my %messages = map {
	my $persistent = 1;
	my $destination = $_;
	map {(++$next_id, POE::Component::MessageQueue::Message->new(
		id          => $next_id,
		timestamp   => ++$when, # We'll fake it so there's a clear time order
		destination => $destination,
		persistent  => $persistent = !$persistent,
		body        => "I am the body of $next_id.\n".  
		               "I was created at $when.\n". 
		               "I am being sent to $destination.\n".
		               'I am '.($persistent ? '' : 'not ')."persistent.\n",
	))} (1..50);
} (@destinations);

sub message_is {
	my ($one, $two, $name) = @_;
	if(ref $one ne 'POE::Component::MessageQueue::Message') {
		return diag "message_is called with non-message argument: ".Dumper($one);
	}
	return ok($one->equals($two), $name) or
	       diag("got: ", Dump($two), "\nexpected:", Dump($one), "\n");
}

sub _run_in_order
{
	my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];
	my $sub_info = $heap->{run_data}->{subs};
	my $this = shift(@$sub_info);
	if ($this) {
		$this->{'sub'}->(@{$this->{args}}, sub {
			$this->{callback}->(@_);
			$kernel->post($session, '_run_in_order');
		});	
	}
	else {
		$heap->{run_data}->{done}->();
	}
}

sub _disown_loop {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];
	$d = $heap->{disown_data};
	if($d->{id} < 50) {
		$d->{storage}->disown($d->{destination}, $d->{id}++, sub {
			$kernel->post($session, '_disown_loop');
		});
	}
	else {
		$d->{done}->();
	}
}

sub _claim_test {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];
	my $d = $heap->{claim_data};
	my $storage = $d->{storage};

	$storage->claim_and_retrieve($d->{destination}, $d->{claim_count}, sub {
		my ($message, $destination, $client_id) = @_;
		if ($message) {
			$d->{claim_count}++;
			$kernel->post($session, '_claim_test');
		}
		else {
			is($d->{claim_count}, 50, "$d->{name}: $destination");
			$d->{done}->();
		}
	});	
}

sub _destination_tests {
	my ($kernel, $session, $heap) = @_[KERNEL, SESSION, HEAP];
	my $data = $heap->{destination_data};
	my $destination = pop(@{$data->{destinations}});
	my $storage = $data->{storage};
	my $name = $data->{name};
	if ($destination) {
		# To sum up:  Claim for this dest (should get 50), disown all, claim again
		# (should again get 50), and disown yet again.
		$heap->{claim_data} = {
			name        => "$name: claim_and_retrieve",
			storage     => $storage,
			destination => $destination,	
			claim_count => 0,
			done        => sub {
				$heap->{disown_data} = {
					storage     => $storage,
					destination => $destination,
					id          => 0,
					done        => sub {
						my $cd = $heap->{claim_data};
						$cd->{claim_count} = 0;
						$cd->{name}        = "$name: disown",
						$cd->{done}        = sub {
							my $dd = $heap->{disown_data};	
							$dd->{id}   = 0;
							$dd->{done} = sub {
								$kernel->post($session, '_destination_tests');
							};
							$kernel->post($session, '_disown_loop');
						};
						$kernel->post($session, '_claim_test');
					},
				};
				$kernel->post($session, '_disown_loop');
			},
		};	
		$kernel->post($session, '_claim_test');
	}
	else {
		$data->{done}->();
	}
}

sub _api_test {
	my ($kernel, $session, $heap, $storage, $name, $done) = 
	  @_[KERNEL,  SESSION,  HEAP, ARG0,     ARG1,  ARG2];

	my $ordered_tests = [
		{ 
			'sub'      => sub { $storage->peek_oldest(@_) }, 
			'args'     => [],
			'callback' => sub { 
				message_is($_[0], $messages{'1'}, "$name: peek_oldest");
			},
		}, 
		{
			'sub'      => sub { $storage->peek(@_) }, 
			'args'     => [['20']],
			'callback' => sub { 
				message_is($_[0]->[0], $messages{'20'}, "$name: peek");
			},
		},
		{
			'sub'      => sub { $storage->remove(@_) }, 
			'args'     => [[qw(20 25 30)]],
			'callback' => sub { 
				my $first = $_[0]->[0];
				message_is($first, $messages{$first->id}, "$name: remove");
			},
		},
		{
			'sub'      => sub { $storage->empty(@_) }, 
			'args'     => [],
			'callback' => sub { 
				is(scalar @{$_[0]}, 197, "$name: empty");
			},
		},
	];
	my @dclone = @destinations;
	$heap->{destination_data} = {
		destinations => \@dclone,
		storage      => $storage,
		name         => $name,
		done => sub {
			$heap->{run_data} = {
				subs => $ordered_tests,
				done => sub {
					delete $heap->{run_data};
					$done->();
				},
			};
			$kernel->post($session, '_run_in_order');
		},
	};
	$kernel->yield('_destination_tests');
}

sub _store_loop {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];
	my $d = $heap->{store_data};
	my $storage = $d->{storage};
	my $message = pop(@{$d->{messages}});
	
	if ($message) {
		$storage->store($message, sub {
			$kernel->post($session, '_store_loop');
		});	
	}
	else {
		$d->{done}->();
	}
}

sub _engine_loop {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];
	my $names = $heap->{engine_data}->{names};
	if (my $name = pop(@$names)) {
		rmtree(DATA_DIR);
		mkpath(DATA_DIR);
		POE::Component::MessageQueue::Storage::Default::_make_db(
			DB_FILE, DSN, q(), q());
		my $storage = make_engine($name);
		my @clones = values %messages;
		$heap->{store_data} = {
			storage  => $storage,
			messages => \@clones,
			done     => sub {
				delete $heap->{store_data};
				$kernel->post($session, '_api_test', $storage, $name, sub {
					$storage->storage_shutdown(sub {
						ok(1, "$name: storage_shutdown");
						$kernel->post($session, '_engine_loop');
					});
				});
			},
		};
		$kernel->yield('_store_loop');	
	}
}

sub _start {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];
	my @names = keys %engines;
	$heap->{engine_data} = {
		names => \@names,
	};
	$kernel->yield('_engine_loop');
}

POE::Session->create(
	inline_states => { 
		_start             => \&_start, 
		_api_test          => \&_api_test,
		_store_loop        => \&_store_loop,
		_claim_test        => \&_claim_test,
		_disown_loop       => \&_disown_loop,
		_engine_loop       => \&_engine_loop,
		_run_in_order      => \&_run_in_order,
		_destination_tests => \&_destination_tests,
	},
);

$poe_kernel->run();
