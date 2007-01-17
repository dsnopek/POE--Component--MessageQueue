
use POE;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Memory;
use strict;

POE::Component::MessageQueue->new({
	storage => POE::Component::MessageQueue::Storage::Memory->new()
});

POE::Kernel->run();
exit;

