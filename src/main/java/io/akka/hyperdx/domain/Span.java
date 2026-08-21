package io.akka.hyperdx.domain;

/**
 * One unit of work reported by one service, SPEC-001 §2.
 *
 * <p>{@code rowId} is the reporter's id for this row and is distinct from {@code spanId}:
 * the same span id can be reported twice by a retrying exporter, and rules 13-14 turn on
 * telling the two apart.
 *
 * <p>{@code parentSpanId} may name a span that is never reported. That is not an error —
 * it is what a sampled-away intermediary looks like from here (rule 6).
 */
public record Span(
    String rowId,
    String traceId,
    String spanId,
    String parentSpanId,
    long timestampNanos,
    long durationNanos,
    String serviceName,
    String spanName,
    String statusCode,
    String sessionId) {

  public boolean hasParent() {
    return parentSpanId != null && !parentSpanId.isEmpty();
  }
}
