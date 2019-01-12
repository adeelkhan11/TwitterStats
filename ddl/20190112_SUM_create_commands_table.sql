CREATE TABLE commands
( id int
, screen_name text
, created_at text
, text text
, processed_date text
, status text);

CREATE UNIQUE INDEX commands_uk on commands (id);
