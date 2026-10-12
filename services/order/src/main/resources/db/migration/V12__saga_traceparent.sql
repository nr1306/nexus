-- The trace the order started in (W3C traceparent, SPEC §9). Replies carry the trace in Kafka headers;
-- work that no message triggers (timeout sweeper, fraud poller) continues it from here.
ALTER TABLE saga_instances ADD COLUMN traceparent text;
