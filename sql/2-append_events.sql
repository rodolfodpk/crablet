CREATE OR REPLACE FUNCTION append_events(
  _domain_ids TEXT[],
  _based_sequence_id BIGINT,
  _event_types TEXT[],
  _event_payloads TEXT[]
) RETURNS BIGINT
  LANGUAGE plpgsql
AS $$
DECLARE
  currentLastSequence BIGINT;
  sequence_id BIGINT;
  event_type TEXT;
  event_payload JSON;
  causation_id BIGINT;
  correlation_id BIGINT;
  previousEventRow events%ROWTYPE;
BEGIN

  -- Sort domain ids
  SELECT ARRAY(SELECT UNNEST(_domain_ids) ORDER BY 1) INTO _domain_ids;

  -- Fetch the row with the max sequence number from the events table
  SELECT * INTO previousEventRow
  FROM events e
  WHERE e.sequence_id = (
    SELECT max(e2.sequence_id)
    FROM events e2
    WHERE e2.domain_ids @> _domain_ids
      AND e2.event_type = ANY(_event_types)
  );

  -- Set the current last sequence if available
  currentLastSequence := previousEventRow.sequence_id;

  -- Initialize causation_id and correlation_id based on the last event
  IF currentLastSequence IS NULL THEN
    causation_id := NULL;  -- No prior event, causation is NULL
    correlation_id := NULL; -- Correlation will be set after the first insert
  ELSE
    causation_id := currentLastSequence;  -- For subsequent events, causation_id points to the previous event's sequence_id
    correlation_id := previousEventRow.correlation_id; -- Same correlation_id as the last event
  END IF;

  -- Lock the transaction using the correlation_id to prevent conflicts
  PERFORM pg_advisory_xact_lock(correlation_id);

  -- Ensure the provided _based_sequence_id matches the current sequence or is null (allowing the first insert)
  IF currentLastSequence IS NULL OR currentLastSequence = _based_sequence_id THEN
    -- Loop through the event types and payloads
    FOR i IN 1 .. array_length(_event_types, 1)
      LOOP
        event_payload := _event_payloads[i]::json;
        event_type := event_payload->>'type';

        -- Insert the event into the database with the appropriate causation and correlation IDs
        INSERT INTO events (event_type, event_payload, domain_ids, causation_id, correlation_id)
        VALUES (event_type, event_payload::json, _domain_ids, causation_id, correlation_id)
        RETURNING events.sequence_id INTO sequence_id;  -- Capture the sequence_id of the inserted event

        -- If it was the first insert (correlation_id was NULL), set correlation_id to the sequence_id of the first event
        IF correlation_id IS NULL THEN
          correlation_id := sequence_id;  -- Set correlation_id to the first inserted event's sequence_id
        END IF;

        -- After the first insert, set causation_id to the sequence_id of the last event inserted
        causation_id := sequence_id;

      END LOOP;
  ELSE
    -- Raise an exception if sequence mismatch is detected
    RAISE EXCEPTION 'Sequence mismatch: the last sequence from the database does not match the supplied lastSequence parameter.';
  END IF;

  -- Return the causation_id of the last event inserted
  RETURN causation_id;
END;
$$;
