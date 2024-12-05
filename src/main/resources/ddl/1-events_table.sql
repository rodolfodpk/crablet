\connect postgres ;

-- TODO
-- https://dev.to/aws-heroes/scalable-sequence-for-postgresql-34o7
-- https://theburningmonk.com/2024/11/eventbridge-best-practice-why-you-should-wrap-events-in-event-envelopes/

CREATE TABLE events
(
    sequence_id    BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_type     VARCHAR(100)                           NOT NULL,
    event_payload  JSON                                   NOT NULL,
    domain_ids     text[]                                 NOT NULL,
    causation_id   BIGINT REFERENCES events (sequence_id),
    correlation_id BIGINT REFERENCES events (sequence_id),
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT domain_ids_length_check CHECK (array_length(domain_ids, 1) BETWEEN 1 AND 7)
);

CREATE INDEX domain_ids_gin_index ON events USING gin (domain_ids);

CREATE INDEX idx_events_event_type ON events(event_type);

CREATE INDEX idx_events_correlation_id ON events(correlation_id);