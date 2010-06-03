
ALTER TABLE messages MODIFY COLUMN persistent enum('1', '0') default '1';

UPDATE meta SET value = '0.2.10' WHERE `key` = 'version';

