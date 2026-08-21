package io.akka.hyperdx.domain;

import java.util.List;

/**
 * One node of the correlated tree: a span, a log record, or the synthetic root that holds
 * what no span can hold (SPEC-001 rule 9).
 *
 * <p>Self-referential by design rather than by flattening — question-log row 2 established
 * by running it that a record holding its own children round-trips through the component
 * client with its nesting preserved.
 */
public record WaterfallNode(
    String rowId,
    Kind kind,
    String spanId,
    String parentSpanId,
    long timestampNanos,
    long durationNanos,
    String serviceName,
    String label,
    String status,
    List<WaterfallNode> children) {

  public enum Kind {
    SPAN,
    LOG,
    /** The synthetic root of rule 9; never reported by anyone, only ever constructed here. */
    UNATTACHED
  }
}
