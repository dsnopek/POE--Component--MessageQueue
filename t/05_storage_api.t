use strict;
use warnings;
no warnings 'recursion'; # some of our loops look like recursion, but aren't
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
				back        => make_engine('Throttled'),
			)}
		},
		BigMemory => {},
		Memory    => {},
	);
}
BEGIN {
	require_ok(MQ_PREFIX.'::Message');
	require_ok(MQ_PREFIX.'::Logger');
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
	my $made = engine_package($name)->new($args->());
	# Suppress all log messages, cause otherwise the output gets cluttered by
	# shutdowns and such.  But this is a great place to put some debugging stuff
	# if things are failing! 
	my $logger = POE::Component::MessageQueue::Logger->new;
	$logger->set_log_function(sub{});
	$made->set_logger($logger);
	return $made;
}

my $next_id = 0;
my $when = time();
my @destinations = map {"/queue/$_"} qw(foo bar baz grapefruit);
my %messages = map {
	my $destination = $_;
	map {(++$next_id, POE::Component::MessageQueue::Message->new(
		id          => $next_id,
		timestamp   => ++$when, # We'll fake it so there's a clear time order
		destination => $destination,
		persistent  => 1,
		body        => "I am the body of $next_id.\n".  
		               "I was created at $when.\n". 
		               "I am being sent to $destination.\n",
	))} (1..50);
} (@destinations);

sub message_is {
	my ($one, $two, $name) = @_;
	if(ref $one ne 'POE::Component::MessageQueue::Message') {
		return diag "message_is called with non-message argument: ".Dump($one);
	}
	return ok($one->equals($two), $name) or
	       diag("got: ", Dump($two), "\nexpected:", Dump($one), "\n");
}

sub run_in_order
{
	my ($tests, $done) = @_;
	if (my $test = shift(@$tests)) {
		$test->{'sub'}->(@{$test->{args}}, sub {
			$test->{callback}->(@_, sub {
				@_ = ($tests, $done);
				goto &run_in_order;
			});
		});
	}
	else {
		goto $done;
	}
}

sub disown_loop {
	my ($storage, $destination, $client, $done) = @_;

	if($client <= 50) {
		$storage->disown_destination($destination, $client, sub {
			@_ = ($storage, $destination, $client+1, $done);
			goto &disown_loop;
		});
	}
	else {
		goto $done;
	}
}

sub claim_test {
	my ($storage, $name, $destination, $count, $done) = @_;

	$storage->claim_and_retrieve($destination, $count, sub {
		if (my $message = $_[0]) {
			@_ = ($storage, $name, $destination, $count+1, $done);
			goto &claim_test;
		}
		else {
			is($count-1, 50, "$name: $destination");
			goto $done;
		}
	});	
}

sub destination_tests {
	my ($destinations, $storage, $name, $done) = @_;
	my $destination = pop(@$destinations) || goto $done;	

	claim_test($storage, "$name: claim_and_retrieve", $destination, 1, sub {
		disown_loop($storage, $destination, 1, sub {
			claim_test($storage, "$name: disown_destination", $destination, 1, sub {
				disown_loop($storage, $destination, 1, sub {
					@_ = ($destinations, $storage, $name, $done);
					goto &destination_tests;
				});
			});
		}); 	
	});
}

sub api_test {
	my ($storage, $name, $done) = @_;

	my $ordered_tests = [
		{ 
			'sub'      => sub { $storage->get_oldest(@_) }, 
			'args'     => [],
			'callback' => sub { 
				my $cb = pop;
				my $msg = shift;
				message_is($msg, $messages{'1'}, "$name: get_oldest");
				goto $cb;
			},
		}, 
		{
			'sub'      => sub { $storage->get(@_) }, 
			'args'     => ['20'],
			'callback' => sub { 
				my $cb = pop;
				my $msg = shift;
				message_is($msg, $messages{'20'}, "$name: get");
				goto $cb;
			},
		},
		{
			'sub'      => sub { $storage->get_all(@_) },
			'args'     => [],
			'callback' => sub {
				my $cb = pop;
				my $aref = shift;
				my $all_equal = 1;
				foreach my $msg (@$aref)
				{
					$all_equal = 0 unless $msg->equals($messages{$msg->id});
				}
				ok($all_equal && @$aref == scalar keys %messages, "$name: get_all");
				goto $cb;
			},
		},
		{
			'sub'      => sub { $storage->claim(@_) },
			'args'     => [1 => 14],
			'callback' => sub {
				my $cb = pop;
				$storage->get(1 => sub {
					my $msg = $_[0];
					is($msg && $msg->claimant, 14, "$name: claim");
					$storage->disown_all(14, $cb);	
				});
			},
		},
		{
			'sub'      => sub { $storage->remove(@_) }, 
			'args'     => [[qw(20 25 30)]],
			'callback' => sub { 
				my $cb = pop;
				$storage->get_all(sub {
					my $messages = $_[0];
					my %hash = map {$_->id => $_} @$messages;
					ok((not exists $hash{'20'}) &&
					   (not exists $hash{'25'}) &&
					   (not exists $hash{'30'}) &&
					   (keys %hash == 197),
					   "$name: remove");
					goto $cb;
				});
			},
		},
		{
			'sub'      => sub { $storage->empty(@_) }, 
			'args'     => [],
			'callback' => sub { 
				my $cb = pop;
				$storage->get_all(sub {
					is(scalar @{$_[0]}, 0, "$name: empty");
					goto $cb;
				});
			},
		},
	];
	my @dclone = @destinations;
	destination_tests(\@dclone, $storage, $name, sub {
		run_in_order($ordered_tests, $done);
	});
}

sub store_loop {
	my ($storage, $messages, $done) = @_;
	my $message = pop(@$messages);
	
	if ($message) {
		$storage->store($message, sub {
			@_ = ($storage, $messages, $done);
			goto &store_loop;
		});	
	}
	else {
		goto $done;
	}
}

sub engine_loop {
	my $names = shift;
	my $name = pop(@$names) || return;
	rmtree(DATA_DIR);
	mkpath(DATA_DIR);
	POE::Component::MessageQueue::Storage::Default::_make_db(
		DB_FILE, DSN, q(), q());

	my $storage = make_engine($name);
	my $clones = [values %messages];

	store_loop($storage, $clones, sub {
		api_test($storage, $name, sub {
			$storage->storage_shutdown(sub {
				ok(1, "$name: storage_shutdown");
				@_ = ($names);
				goto &engine_loop;	
			});
		});
	});
}

POE::Session->create(
	inline_states => { _start => sub {
		my $arg = $ARGV[0];
		engine_loop([$arg ? $arg : keys %engines])
	}},
);

$poe_kernel->run();
