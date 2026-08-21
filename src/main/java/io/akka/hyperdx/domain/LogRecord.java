package io.akka.hyperdx.domain;

/**
 * One log line, SPEC-001 §2.
 *
 * <p>{@code spanId} is optional: a line emitted outside any span carries none, and rule 10
 * places it rather than dropping it.
 */
public record LogRecord(
    String rowId,
    String traceId,
    String spanId,
    long timestampNanos,
    String body,
    String severityText) {

  public boolean hasSpan() {
    return spanId != null && !spanId.isEmpty();
  }
}
