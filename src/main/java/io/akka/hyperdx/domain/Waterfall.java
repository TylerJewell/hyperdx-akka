package io.akka.hyperdx.domain;

import java.util.List;

/**
 * The correlated answer for one trace, SPEC-001 §3.
 *
 * <p>{@code roots} carries the nested tree — a node holds its own children, which survives
 * the component-client round trip intact (question-log row 2), so nothing is flattened for
 * transport. {@code flattened} is that same tree in traversal order, which is the order a
 * waterfall is drawn in.
 *
 * <p>The counts are the whole of decision 1 and decision 3: a row that could not be placed
 * is placed under {@link #UNATTACHED_ROOT_ID} and counted here rather than disappearing.
 * {@code unreachableSpanCount} is the ring case of rule 9b — spans that name each other as
 * parents and so contain no root.
 */
public record Waterfall(
    String traceId,
    List<WaterfallNode> roots,
    List<FlatNode> flattened,
    int unattachedLogCount,
    int duplicateSpanCount,
    int unreachableSpanCount,
    int spanCount,
    int logCount) {

  /** The id of the synthetic root that holds rows no reported span can hold (rule 9). */
  public static final String UNATTACHED_ROOT_ID = "__unattached__";

  /** One node in traversal order, carrying the depth it was found at. */
  public record FlatNode(int level, String rowId, String kind, String spanId, long timestampNanos, String label) {}
}
