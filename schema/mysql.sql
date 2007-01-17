
CREATE TABLE messages
(
	message_id  int primary key,
	destination varchar(255) not null,
	persistent  enum('Y', 'N') default 'Y' not null,
	in_use_by   int,
	body        text
);

