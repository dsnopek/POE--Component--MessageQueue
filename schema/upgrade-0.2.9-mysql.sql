
ALTER TABLE messages MODIFY COLUMN timestamp decimal(15,5);

UPDATE meta SET value = '0.2.9' WHERE `key` = 'version';

