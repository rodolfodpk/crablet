CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION get_hash(_ids TEXT[]) RETURNS INT AS $$
DECLARE
    _temp text;
BEGIN
    SELECT array_to_string(_ids, '') INTO _temp;
    RETURN ('x' || SUBSTRING(ENCODE(DIGEST(_temp, 'sha256'), 'hex') FROM 1 FOR 8))::bit(32)::int;
END;
$$ LANGUAGE plpgsql;