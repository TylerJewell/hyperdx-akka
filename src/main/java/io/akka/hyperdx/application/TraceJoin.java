package io.akka.hyperdx.application;

import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.Waterfall;
import io.akka.hyperdx.domain.WaterfallNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The correlation itself: spans and log records reported for one trace, in any order, into
 * one ordered tree. SPEC-001 §3 rules 1-14.
 *
 * <p>Pure and static — no state between calls, no clock — so the same rows always give the
 * same answer. That is rule 3, and it is what lets the arrival-order property be tested by
 * exhausting permutations rather than sampling them.
 *
 * <p>The shape is: order everything totally, then place each row in one pass. Ordering
 * first is what makes the pass order-independent. "The first report of this span id" means
 * first by timestamp rather than first to arrive, and a child can be placed under a parent
 * that appears later in the input because placement is by id, not by position.
 */
public final class TraceJoin {

  private TraceJoin() {}

  /** A span or a log record reduced to what ordering and placement need. */
  private record Row(String rowId, boolean isSpan, long timestampNanos, Span span, LogRecord log) {
    static Row of(Span s) {
      return new Row(s.rowId(), true, s.timestampNanos(), s, null);
    }

    static Row of(LogRecord l) {
      return new Row(l.rowId(), false, l.timestampNanos(), null, l);
    }
  }

  private static final Comparator<Row> ROW_ORDER =
      Comparator.comparingLong(Row::timestampNanos)
          // Rule 2: spans before logs at the identical instant, then by row id, so the
          // order is total and no pair is left to whatever the sort happened to do.
          .thenComparingInt(r -> r.isSpan() ? 0 : 1)
          .thenComparing(Row::rowId);

  public static Waterfall correlate(String traceId, List<Span> spans, List<LogRecord> logs) {
    var rows = new ArrayList<Row>(spans.size() + logs.size());
    spans.forEach(s -> rows.add(Row.of(s)));
    logs.forEach(l -> rows.add(Row.of(l)));
    rows.sort(ROW_ORDER);

    // Rule 6 turns on "was this parent reported at all", which cannot be answered while
    // still walking the rows — hence a pass of its own.
    Set<String> reportedSpanIds = new HashSet<>();
    for (var r : rows) {
      if (r.isSpan() && hasId(r.span())) reportedSpanIds.add(r.span().spanId());
    }

    var canonicalRowIdBySpanId = new LinkedHashMap<String, String>();
    var rootSpans = new ArrayList<Span>();
    // Insertion-ordered, and rows are already in ROW_ORDER, so a parent's children come out
    // ordered by rule 1 without a second sort.
    var childrenOf = new LinkedHashMap<String, List<Row>>();
    var unattached = new ArrayList<WaterfallNode>();
    int duplicateSpans = 0;
    int unattachedLogs = 0;

    for (var r : rows) {
      if (r.isSpan()) {
        var s = r.span();
        if (!hasId(s)) {
          // A span with no id of its own can hold nothing and be held by nothing.
          unattached.add(spanNode(s, List.of()));
          continue;
        }
        if (canonicalRowIdBySpanId.containsKey(s.spanId())) {
          duplicateSpans++; // Rules 13-14: the first by ROW_ORDER wins; this one is counted.
          continue;
        }
        canonicalRowIdBySpanId.put(s.spanId(), s.rowId());
        if (!s.hasParent() || !reportedSpanIds.contains(s.parentSpanId())) {
          rootSpans.add(s); // Rules 6-7
        } else {
          childrenOf.computeIfAbsent(s.parentSpanId(), k -> new ArrayList<>()).add(r);
        }
      } else {
        var l = r.log();
        if (!l.hasSpan() || !reportedSpanIds.contains(l.spanId())) {
          unattached.add(logNode(l)); // Rules 9-10
          unattachedLogs++;
        } else {
          childrenOf.computeIfAbsent(l.spanId(), k -> new ArrayList<>()).add(r); // Rule 4
        }
      }
    }

    var rootNodes = new ArrayList<WaterfallNode>(rootSpans.size() + 1);
    var reached = new HashSet<String>();
    for (var root : rootSpans) rootNodes.add(build(root, childrenOf, reached));

    // A parent link may point forward, so a set of spans can name each other in a ring and
    // contain no root. Nothing above would then reach them, and rule 9b says they are shown
    // rather than lost. Every placed row not reached from a root goes under the synthetic
    // root as a leaf, in ROW_ORDER, which is the order `rows` is already in.
    int unreachableSpans = 0;
    for (var r : rows) {
      if (r.isSpan()) {
        var s = r.span();
        if (!hasId(s) || !s.rowId().equals(canonicalRowIdBySpanId.get(s.spanId()))) continue;
        if (reached.add(s.rowId())) {
          unattached.add(spanNode(s, List.of()));
          unreachableSpans++;
        }
      } else {
        var l = r.log();
        if (!l.hasSpan() || !reportedSpanIds.contains(l.spanId())) continue;
        if (reached.add(l.rowId())) {
          unattached.add(logNode(l));
          unattachedLogs++;
        }
      }
    }

    if (!unattached.isEmpty()) { // Rule 12: only when there is something to put under it
      rootNodes.add(new WaterfallNode(
          Waterfall.UNATTACHED_ROOT_ID, WaterfallNode.Kind.UNATTACHED, null, null,
          unattached.get(0).timestampNanos(), 0, null,
          "rows no reported span could hold", null, List.copyOf(unattached)));
    }

    var flat = new ArrayList<Waterfall.FlatNode>();
    for (var n : rootNodes) flatten(n, 0, flat);

    return new Waterfall(traceId, List.copyOf(rootNodes), List.copyOf(flat),
        unattachedLogs, duplicateSpans, unreachableSpans, spans.size(), logs.size());
  }

  /**
   * {@code reached} collects every row id this traversal placed, both so that the caller can
   * find what no root reaches (rule 9b) and so that a ring of spans naming each other
   * terminates instead of recursing forever.
   */
  private static WaterfallNode build(Span span, Map<String, List<Row>> childrenOf, Set<String> reached) {
    reached.add(span.rowId());
    var kids = childrenOf.getOrDefault(span.spanId(), List.of());
    var children = new ArrayList<WaterfallNode>(kids.size());
    for (var r : kids) {
      if (!reached.add(r.rowId())) continue;
      children.add(r.isSpan() ? build(r.span(), childrenOf, reached) : logNode(r.log()));
    }
    return spanNode(span, children);
  }

  private static boolean hasId(Span s) {
    return s.spanId() != null && !s.spanId().isEmpty();
  }

  private static WaterfallNode spanNode(Span s, List<WaterfallNode> children) {
    return new WaterfallNode(s.rowId(), WaterfallNode.Kind.SPAN, s.spanId(), s.parentSpanId(),
        s.timestampNanos(), s.durationNanos(), s.serviceName(), s.spanName(), s.statusCode(),
        List.copyOf(children));
  }

  private static WaterfallNode logNode(LogRecord l) {
    return new WaterfallNode(l.rowId(), WaterfallNode.Kind.LOG, l.spanId(), null,
        l.timestampNanos(), 0, null, l.body(), l.severityText(), List.of());
  }

  private static void flatten(WaterfallNode node, int level, List<Waterfall.FlatNode> out) {
    out.add(new Waterfall.FlatNode(level, node.rowId(), node.kind().name(), node.spanId(),
        node.timestampNanos(), node.label()));
    for (var c : node.children()) flatten(c, level + 1, out);
  }
}
