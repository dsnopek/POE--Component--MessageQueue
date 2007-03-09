
use POE;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::Memory;
use POE::Component::MessageQueue::Logger;
use strict;

# Force some logger output without using the real logger.
$POE::Component::MessageQueue::Logger::LEVEL = 0;

POE::Component::MessageQueue->new({
	storage => POE::Component::MessageQueue::Storage::Memory->new()
});

POE::Kernel->run();
exit;

