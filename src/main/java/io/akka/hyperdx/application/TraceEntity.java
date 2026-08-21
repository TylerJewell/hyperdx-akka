package io.akka.hyperdx.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.TraceEvent;
import io.akka.hyperdx.domain.TraceState;
import io.akka.hyperdx.domain.Waterfall;
import java.util.List;

/**
 * One trace: the spans and log records reported under it, and the correlated waterfall
 * built from them on demand.
 *
 * <p>Addressed by trace id, so every row for a trace lands on one entity and the answer is
 * read-your-writes — a span reported and a waterfall asked for immediately afterwards see
 * each other. That is what makes SPEC-001 rule 3 testable end to end rather than only
 * inside {@link TraceJoin}: arrival order is genuinely a sequence of separate calls, not a
 * shuffled list.
 *
 * <p>Rows are kept as reported and joined at read time. Joining on the way in would have to
 * decide where a row belongs before the row that answers that question has arrived, which
 * is the fault the whole slice is about.
 */
@Component(id = "trace")
public class TraceEntity extends EventSourcedEntity<TraceState, TraceEvent> {

  /**
   * The row ceiling for one trace. State replicates across regions only while it stays
   * under a megabyte, and a trace is an append-only accumulation with no natural end, so
   * without a bound the ceiling is reached eventually and silently. Rows are a few hundred
   * bytes each; ten thousand of them is the right order of magnitude below the limit, and
   * is far above any trace a person reads as a waterfall.
   */
  public static final int MAX_ROWS = 10_000;

  private final String traceId;

  public TraceEntity(EventSourcedEntityContext context) {
    this.traceId = context.entityId();
  }

  @Override
  public TraceState emptyState() {
    return TraceState.empty();
  }

  public Effect<Done> reportSpans(List<Span> spans) {
    if (spans == null || spans.isEmpty()) {
      return effects().reply(Done.getInstance());
    }
    if (currentState().rowCount() + spans.size() > MAX_ROWS) {
      return effects().error(fullMessage(spans.size()));
    }
    return effects().persist(new TraceEvent.SpansReported(spans)).thenReply(s -> Done.getInstance());
  }

  public Effect<Done> reportLogs(List<LogRecord> logs) {
    if (logs == null || logs.isEmpty()) {
      return effects().reply(Done.getInstance());
    }
    if (currentState().rowCount() + logs.size() > MAX_ROWS) {
      return effects().error(fullMessage(logs.size()));
    }
    return effects().persist(new TraceEvent.LogsReported(logs)).thenReply(s -> Done.getInstance());
  }

  /** Names the limit and the numbers, and carries no row content — the ids are the
   * reporter's, but bodies and attributes are not put in an error message. */
  private String fullMessage(int offered) {
    return "trace holds " + currentState().rowCount() + " rows and " + offered
        + " more were offered; the ceiling is " + MAX_ROWS;
  }

  public ReadOnlyEffect<Waterfall> waterfall() {
    var s = currentState();
    return effects().reply(TraceJoin.correlate(traceId, s.spans(), s.logs()));
  }

  /** The session ids named by the spans reported so far, for the session correlation. */
  public ReadOnlyEffect<List<String>> sessionIds() {
    return effects().reply(currentState().spans().stream()
        .map(Span::sessionId)
        .filter(id -> id != null && !id.isEmpty())
        .distinct()
        .sorted()
        .toList());
  }

  @Override
  public TraceState applyEvent(TraceEvent event) {
    return switch (event) {
      case TraceEvent.SpansReported e -> currentState().withSpans(traceId, e.spans());
      case TraceEvent.LogsReported e -> currentState().withLogs(traceId, e.logs());
    };
  }
}
