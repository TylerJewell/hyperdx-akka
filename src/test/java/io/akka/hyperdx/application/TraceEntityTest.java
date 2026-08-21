package io.akka.hyperdx.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.TraceState;
import io.akka.hyperdx.domain.Waterfall;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** What the trace entity adds on top of {@link TraceJoin}: idempotent ingest and a ceiling. */
public class TraceEntityTest {

  private static Span span(String rowId, String spanId, String parent, long ts) {
    return new Span(rowId, "T", spanId, parent, ts, 1, "svc", spanId, "Ok", null);
  }

  @Test
  public void ignoresARedeliveredRowRatherThanCountingItAsADuplicateSpan() {
    var kit = EventSourcedTestKit.<TraceState, io.akka.hyperdx.domain.TraceEvent, TraceEntity>of(
        "T", TraceEntity::new);
    var s = span("r1", "A", "", 10L);
    kit.method(TraceEntity::reportSpans).invoke(List.of(s));
    kit.method(TraceEntity::reportSpans).invoke(List.of(s));

    Waterfall w = kit.method(TraceEntity::waterfall).invoke().getReply();
    assertEquals(1, w.spanCount());
    assertEquals(0, w.duplicateSpanCount(),
        "a redelivered row is a fault in the transport, not a duplicate span in the traced system");
  }

  @Test
  public void stillCountsAGenuineDuplicateSpanIdReportedUnderADifferentRowId() {
    var kit = EventSourcedTestKit.<TraceState, io.akka.hyperdx.domain.TraceEvent, TraceEntity>of(
        "T", TraceEntity::new);
    kit.method(TraceEntity::reportSpans).invoke(List.of(span("r1", "A", "", 10L)));
    kit.method(TraceEntity::reportSpans).invoke(List.of(span("r2", "A", "", 20L)));

    Waterfall w = kit.method(TraceEntity::waterfall).invoke().getReply();
    assertEquals(1, w.duplicateSpanCount());
  }

  @Test
  public void refusesAReportThatWouldTakeTheTracePastItsCeilingAndSaysWhatTheCeilingIs() {
    var kit = EventSourcedTestKit.<TraceState, io.akka.hyperdx.domain.TraceEvent, TraceEntity>of(
        "T", TraceEntity::new);
    var many = IntStream.range(0, TraceEntity.MAX_ROWS)
        .mapToObj(i -> span("r" + i, "s" + i, "", i))
        .toList();
    kit.method(TraceEntity::reportSpans).invoke(many);

    var refused = kit.method(TraceEntity::reportLogs)
        .invoke(List.of(new LogRecord("one-too-many", "T", "s0", 1L, "body", "info")));
    assertTrue(refused.isError());
    assertTrue(refused.getError().contains(String.valueOf(TraceEntity.MAX_ROWS)),
        "the message names the ceiling: " + refused.getError());
    assertEquals(TraceEntity.MAX_ROWS,
        kit.method(TraceEntity::waterfall).invoke().getReply().spanCount());
  }
}
