package io.akka.hyperdx.domain;

/**
 * One session-replay event whose chunks all arrived and whose body parsed.
 *
 * <p>{@code timestampNanos} is the timestamp of the event's *first* chunk (rule 20), not of
 * whichever chunk happened to arrive last — the ordering of the emitted stream must not
 * depend on arrival order any more than the waterfall's does.
 */
public record ReplayEvent(String eventKey, long timestampNanos, String body) {}
