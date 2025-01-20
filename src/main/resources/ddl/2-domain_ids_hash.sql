CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION get_hash(_ids TEXT[]) RETURNS INT AS $$
DECLARE
    _temp TEXT;
BEGIN
    -- Handle NULL or empty input
    IF _ids IS NULL OR array_length(_ids, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    -- Sort array elements and concatenate with a separator to avoid collision
    SELECT array_to_string(ARRAY(SELECT UNNEST(_ids) ORDER BY 1), ',') INTO _temp;

    -- Hash and return an integer representation
    RETURN ('x' || LEFT(ENCODE(DIGEST(_temp, 'sha256'), 'hex'), 8))::bit(32)::int;
END;
$$ LANGUAGE plpgsql;