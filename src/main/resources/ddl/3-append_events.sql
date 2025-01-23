CREATE OR REPLACE PROCEDURE append_events(
    IN _domain_ids TEXT[],
    IN _expected_sequence_id BIGINT,
    IN _event_types TEXT[],
    IN _event_payloads TEXT[],
    IN _lock_policy INT
)
    LANGUAGE plpgsql
AS $$
DECLARE
    _lastEventSequenceId    BIGINT;
    _lastEventCorrelationId BIGINT;
    _currentEventType       TEXT;
    _currentEventPayload    JSON;
    _causationId            BIGINT;
    _correlationId          BIGINT;
    _isLockAcquired         BOOLEAN;
    _newSequenceIds         BIGINT[];
BEGIN

    -- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    RAISE NOTICE 'Current transaction isolation level is: %', current_setting('transaction_isolation');

    -- Sort domain ids
    SELECT ARRAY(SELECT UNNEST(_domain_ids) ORDER BY 1) INTO _domain_ids;

    -- Fetch the row with the max sequence number from the events table
    SELECT events.sequence_id,
           events.correlation_id
    INTO
        _lastEventSequenceId, _lastEventCorrelationId
    FROM events
    WHERE sequence_id = (SELECT max(e2.sequence_id)
                         FROM events e2
                         WHERE e2.domain_ids @> _domain_ids
                           AND e2.event_type = ANY (_event_types));

    -- Initialize _causationId and _correlationId based on the last event
    IF _lastEventSequenceId IS NULL THEN
        SELECT ARRAY(SELECT nextval('events_sequence_id_seq') FROM generate_series(1, array_length(_event_payloads, 1)))
        INTO _newSequenceIds;
        _causationId := _newSequenceIds[1];
        _correlationId := _causationId;
    ELSE
        _causationId := _lastEventSequenceId;
        _correlationId := _lastEventCorrelationId; -- Same correlation_id as the last event

        -- Locking mechanism based on _lock_policy
        IF _lock_policy = 1 THEN
            _isLockAcquired := pg_try_advisory_xact_lock(get_hash(_domain_ids));
        ELSIF _lock_policy = 2 THEN
            _isLockAcquired := pg_try_advisory_xact_lock(_lastEventSequenceId);
        ELSIF _lock_policy = 3 THEN
            _isLockAcquired := pg_try_advisory_xact_lock(_lastEventCorrelationId);
        END IF;

        IF NOT _isLockAcquired THEN
            IF _lock_policy = 1 THEN
                RAISE EXCEPTION 'Failed to acquire lock for _domain_ids: %', _domain_ids;
            ELSIF _lock_policy = 2 THEN
                RAISE EXCEPTION 'Failed to acquire lock for _lastEventSequenceId: %', _lastEventSequenceId;
            ELSIF _lock_policy = 3 THEN
                RAISE EXCEPTION 'Failed to acquire lock for _lastEventCorrelationId: %', _lastEventCorrelationId;
            END IF;
        END IF;

        SELECT ARRAY(SELECT nextval('events_sequence_id_seq') FROM generate_series(1, array_length(_event_payloads, 1)))
        INTO _newSequenceIds;
    END IF;

    -- Ensure the provided _expected_sequence_id matches the current sequence or is null (first insert allowed)
    IF _lastEventSequenceId IS NULL OR _lastEventSequenceId = _expected_sequence_id THEN
        -- Loop through the event payloads and insert into the table
        FOR i IN 1 .. array_length(_event_payloads, 1)
            LOOP
                _currentEventPayload := _event_payloads[i]::json;
                _currentEventType := _currentEventPayload ->> 'type';

                INSERT INTO events (sequence_id, event_type, event_payload, domain_ids, causation_id, correlation_id)
                VALUES (_newSequenceIds[i], _currentEventType, _currentEventPayload, _domain_ids,
                        _causationId, _correlationId);

                _causationId := _newSequenceIds[i];
            END LOOP;
    ELSE
        -- Raise an exception if sequence mismatch is detected
        RAISE EXCEPTION 'Sequence mismatch: the current last sequence % from the database does not match the expected sequence: %.',
            _lastEventSequenceId, _expected_sequence_id;
    END IF;

--     COMMIT;
--
-- EXCEPTION WHEN others THEN
--     ROLLBACK;
END;
$$;
