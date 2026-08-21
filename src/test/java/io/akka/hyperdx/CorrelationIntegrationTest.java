package io.akka.hyperdx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.testkit.TestKitSupport;
import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.Waterfall;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The rules driven through a started runtime and the HTTP surface, where arrival order is
 * genuinely a sequence of separate calls rather than the order of a list.
 */
public class CorrelationIntegrationTest extends TestKitSupport {

  private static Span span(String rowId, String traceId, String spanId, String parent, long ts, String sessionId) {
    return new Span(rowId, traceId, spanId, parent, ts, 1000, "svc", spanId, "Ok", sessionId);
  }

  private static LogRecord log(String rowId, String traceId, String spanId, long ts) {
    return new LogRecord(rowId, traceId, spanId, ts, "body-" + rowId, "info");
  }

  private static String shape(Waterfall w) {
    var sb = new StringBuilder();
    for (var n : w.flattened()) {
      sb.append("  ".repeat(n.level())).append(n.kind()).append(':').append(n.rowId()).append('\n');
    }
    return sb.toString();
  }

  private Waterfall waterfall(String traceId) {
    var res = httpClient.GET("/traces/" + traceId + "/waterfall").responseBodyAs(Waterfall.class).invoke();
    assertEquals(StatusCodes.OK, res.status());
    return res.body();
  }

  private void postSpans(String traceId, List<Span> spans) {
    var res = httpClient.POST("/traces/" + traceId + "/spans").withRequestBody(spans).invoke();
    assertEquals(StatusCodes.OK, res.status());
  }

  private void postLogs(String traceId, List<LogRecord> logs) {
    var res = httpClient.POST("/traces/" + traceId + "/logs").withRequestBody(logs).invoke();
    assertEquals(StatusCodes.OK, res.status());
  }

  /** Rule 5 over separate calls: the child is reported, answered on, then the parent arrives. */
  @Test
  public void aChildReportedAndAnsweredOnBeforeItsParentIsAdoptedWhenTheParentArrives() {
    var traceId = "t-adopt";
    postSpans(traceId, List.of(span("child", traceId, "B", "A", 10L, null)));

    var before = waterfall(traceId);
    assertEquals("SPAN:child\n", shape(before),
        "with no parent reported yet the child is a root of its own — rule 6");

    postSpans(traceId, List.of(span("parent", traceId, "A", "", 20L, null)));
    assertEquals("SPAN:parent\n  SPAN:child\n", shape(waterfall(traceId)));
  }

  /** Rule 3 across calls: the same rows reported in the opposite order give the same answer. */
  @Test
  public void twoTracesGivenTheSameRowsInOppositeOrdersAgree() {
    var rows = List.of(
        span("s-root", "X", "A", "", 10L, null),
        span("s-mid", "X", "B", "A", 20L, null),
        span("s-leaf", "X", "C", "B", 30L, null));
    var logs = List.of(log("l-in", "X", "B", 25L), log("l-orphan", "X", "ZZ", 26L));

    postSpans("t-forward", rows.stream().map(s -> retrace(s, "t-forward")).toList());
    postLogs("t-forward", logs.stream().map(l -> retrace(l, "t-forward")).toList());

    var reversedSpans = new java.util.ArrayList<>(rows);
    java.util.Collections.reverse(reversedSpans);
    var reversedLogs = new java.util.ArrayList<>(logs);
    java.util.Collections.reverse(reversedLogs);
    // Logs first this time, and each row in its own call — a different arrival order in
    // every sense the runtime can express.
    for (var l : reversedLogs) postLogs("t-reverse", List.of(retrace(l, "t-reverse")));
    for (var s : reversedSpans) postSpans("t-reverse", List.of(retrace(s, "t-reverse")));

    assertEquals(shape(waterfall("t-forward")), shape(waterfall("t-reverse")));
    assertEquals(waterfall("t-forward").unattachedLogCount(), waterfall("t-reverse").unattachedLogCount());
  }

  private static Span retrace(Span s, String traceId) {
    return new Span(s.rowId(), traceId, s.spanId(), s.parentSpanId(), s.timestampNanos(),
        s.durationNanos(), s.serviceName(), s.spanName(), s.statusCode(), s.sessionId());
  }

  private static LogRecord retrace(LogRecord l, String traceId) {
    return new LogRecord(l.rowId(), traceId, l.spanId(), l.timestampNanos(), l.body(), l.severityText());
  }

  /** Rule 11 over the wire: a trace made only of logs still answers with its logs. */
  @Test
  public void aTraceMadeOnlyOfLogsAnswersWithThoseLogs() {
    var traceId = "t-logs-only";
    postLogs(traceId, List.of(log("l1", traceId, "A", 10L), log("l2", traceId, "B", 20L)));
    var w = waterfall(traceId);
    assertEquals("UNATTACHED:" + Waterfall.UNATTACHED_ROOT_ID + "\n  LOG:l1\n  LOG:l2\n", shape(w));
    assertEquals(2, w.unattachedLogCount());
  }

  /** Rules 21-22: the session learns of the trace whichever way round the two are reported. */
  @Test
  public void aSessionLearnsOfItsTraceWhicheverOrderTheChunksAndSpansArrive() {
    var body = "{\"type\":2,\"timestamp\":1700000000000}";

    // Chunks first, then the span that names the session.
    postChunks("sess-a", List.of(new ReplayChunk("sess-a", "e1", 0, 0, body, 100L)));
    postSpans("t-sess-a", List.of(span("s", "t-sess-a", "A", "", 10L, "sess-a")));
    assertEquals(List.of("t-sess-a"), replay("sess-a").traceIds());

    // Span first, then the chunks.
    postSpans("t-sess-b", List.of(span("s", "t-sess-b", "A", "", 10L, "sess-b")));
    postChunks("sess-b", List.of(new ReplayChunk("sess-b", "e1", 0, 0, body, 100L)));
    var r = replay("sess-b");
    assertEquals(List.of("t-sess-b"), r.traceIds());
    assertEquals(1, r.events().size());
  }

  /** Rules 16-17 over separate calls: a chunk reported later completes an event that was
   * incomplete when it was last asked for. */
  @Test
  public void anEventIncompleteAtOneReadIsCompleteOnceItsMissingChunkArrives() {
    var sessionId = "sess-late";
    postChunks(sessionId, List.of(
        new ReplayChunk(sessionId, "e1", 3, 3, "\"x\":1}}", 300L),
        new ReplayChunk(sessionId, "e1", 1, 3, "{\"type\":2,", 100L)));

    var first = replay(sessionId);
    assertTrue(first.events().isEmpty());
    assertEquals(1, first.incomplete().size());
    assertEquals(List.of(2), first.incomplete().get(0).missingIndices());

    postChunks(sessionId, List.of(new ReplayChunk(sessionId, "e1", 2, 3, "\"data\":{", 200L)));
    var second = replay(sessionId);
    assertEquals(1, second.events().size());
    assertEquals("{\"type\":2,\"data\":{\"x\":1}}", second.events().get(0).body());
    assertEquals(100L, second.events().get(0).timestampNanos());
    assertTrue(second.incomplete().isEmpty());
  }

  private void postChunks(String sessionId, List<ReplayChunk> chunks) {
    var res = httpClient.POST("/traces/sessions/" + sessionId + "/chunks").withRequestBody(chunks).invoke();
    assertEquals(StatusCodes.OK, res.status());
  }

  private Replay replay(String sessionId) {
    var res = httpClient.GET("/traces/sessions/" + sessionId + "/replay")
        .responseBodyAs(Replay.class).invoke();
    assertEquals(StatusCodes.OK, res.status());
    return res.body();
  }
}
