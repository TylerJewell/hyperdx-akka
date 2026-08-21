package io.akka.hyperdx.domain;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Every row reported for one trace, kept as reported.
 *
 * <p>Nothing is discarded on the way in on the grounds of where it belongs — not a
 * duplicate span id, not a log whose span never arrived. The rules that decide what to show
 * are applied when the waterfall is asked for, not when a row lands, because a row that
 * cannot be placed now may be placeable once the span it names arrives (SPEC-001 rules 3,
 * 5).
 *
 * <p>A row whose {@code rowId} has already been reported is ignored. `rowId` is the
 * reporter's unique id for the row, so a second copy of it is a redelivery, not a second
 * row — and counting it as a duplicate span (rule 14) would report a fault in the traced
 * system that is in fact a fault in the transport.
 */
public record TraceState(String traceId, List<Span> spans, List<LogRecord> logs, Set<String> rowIds) {

  public static TraceState empty() {
    return new TraceState(null, List.of(), List.of(), Set.of());
  }

  public int rowCount() {
    return spans.size() + logs.size();
  }

  public TraceState withSpans(String id, List<Span> more) {
    var seen = new LinkedHashSet<>(rowIds);
    var next = new ArrayList<>(spans);
    for (var s : more) {
      if (seen.add(s.rowId())) next.add(s);
    }
    return new TraceState(traceId == null ? id : traceId, List.copyOf(next), logs, Set.copyOf(seen));
  }

  public TraceState withLogs(String id, List<LogRecord> more) {
    var seen = new LinkedHashSet<>(rowIds);
    var next = new ArrayList<>(logs);
    for (var l : more) {
      if (seen.add(l.rowId())) next.add(l);
    }
    return new TraceState(traceId == null ? id : traceId, spans, List.copyOf(next), Set.copyOf(seen));
  }
}
