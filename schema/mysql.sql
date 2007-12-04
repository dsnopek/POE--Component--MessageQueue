
CREATE TABLE messages
(
	message_id  int primary key,
	destination varchar(255) not null,
	persistent  enum('Y', 'N') default 'Y' not null,
	in_use_by   int,
	body        text,
	timestamp   int,
	size        int
);

-- Improves performance some bit:
CREATE INDEX destination_index ON messages ( destination );
CREATE INDEX in_use_by_index   ON messages ( in_use_by );

