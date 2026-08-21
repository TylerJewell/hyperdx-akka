package io.akka.hyperdx.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.Waterfall;
import io.akka.hyperdx.domain.WaterfallNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 1-14, one test per rule or per pair of rules that cannot be separated. */
public class TraceJoinTest {

  private static Span span(String rowId, String spanId, String parentSpanId, long ts) {
    return new Span(rowId, "T", spanId, parentSpanId, ts, 1000, "svc", spanId, "Ok", null);
  }

  private static LogRecord log(String rowId, String spanId, long ts) {
    return new LogRecord(rowId, "T", spanId, ts, "body-" + rowId, "info");
  }

  /** The tree as indented text, which is what a waterfall is: order plus depth. */
  private static String shape(Waterfall w) {
    var sb = new StringBuilder();
    for (var n : w.flattened()) {
      sb.append("  ".repeat(n.level())).append(n.kind()).append(':').append(n.rowId()).append('\n');
    }
    return sb.toString();
  }

  // Rule 1
  @Test
  public void ordersAtNanosecondPrecisionNotMilliseconds() {
    var w = TraceJoin.correlate("T",
        // Row ids chosen so the two orderings disagree: truncated to milliseconds these
        // rows tie, and a tie falls through to the row id, which puts them the other way
        // round from the nanosecond order.
        List.of(span("a-later", "A", "", 1_000_000_500L), span("b-earlier", "B", "", 1_000_000_100L)),
        List.of());
    assertEquals("SPAN:b-earlier\nSPAN:a-later\n", shape(w));
  }

  // Rule 2
  @Test
  public void breaksAnIdenticalInstantWithSpansFirstThenRowId() {
    var w = TraceJoin.correlate("T",
        List.of(span("s2", "B", "", 5L), span("s1", "A", "", 5L)),
        List.of(log("l1", null, 5L)));
    assertEquals("SPAN:s1\nSPAN:s2\nUNATTACHED:" + Waterfall.UNATTACHED_ROOT_ID + "\n  LOG:l1\n", shape(w));
  }

  // Rule 3 — the rule the whole slice exists for.
  @Test
  public void givesTheSameAnswerForEveryArrivalOrderOfTheSameRows() {
    var spans = List.of(
        span("s-root", "A", "", 10L),
        span("s-mid", "B", "A", 20L),
        span("s-leaf", "C", "B", 30L),
        span("s-gap", "D", "NEVER-REPORTED", 40L));
    var logs = List.of(log("l-in", "B", 25L), log("l-orphan", "ZZ", 26L), log("l-nospan", null, 27L));

    var expected = shape(TraceJoin.correlate("T", spans, logs));
    for (var permutation : permutations(spans)) {
      for (var logPermutation : permutations(logs)) {
        assertEquals(expected, shape(TraceJoin.correlate("T", permutation, logPermutation)),
            "arrival order " + permutation.stream().map(Span::rowId).toList()
                + " / " + logPermutation.stream().map(LogRecord::rowId).toList()
                + " gave a different answer");
      }
    }
  }

  private static <X> List<List<X>> permutations(List<X> items) {
    if (items.isEmpty()) return List.of(List.of());
    var out = new ArrayList<List<X>>();
    for (int i = 0; i < items.size(); i++) {
      var rest = new ArrayList<>(items);
      var head = rest.remove(i);
      for (var tail : permutations(rest)) {
        var one = new ArrayList<X>();
        one.add(head);
        one.addAll(tail);
        out.add(one);
      }
    }
    return out;
  }

  // Rules 4-5
  @Test
  public void nestsALogUnderItsSpanAndAChildUnderAParentReportedAfterIt() {
    var w = TraceJoin.correlate("T",
        List.of(span("child", "B", "A", 10L), span("parent", "A", "", 20L)),
        List.of(log("l", "B", 30L)));
    assertEquals("SPAN:parent\n  SPAN:child\n    LOG:l\n", shape(w));
  }

  // Rules 6-7
  @Test
  public void makesASpanWithAMissingOrAbsentParentARootAndKeepsItsChildren() {
    var w = TraceJoin.correlate("T",
        List.of(span("gap", "B", "NEVER-REPORTED", 10L), span("kid", "C", "B", 20L)),
        List.of());
    assertEquals("SPAN:gap\n  SPAN:kid\n", shape(w));
    assertEquals(1, w.roots().size());
  }

  // Rule 8
  @Test
  public void returnsEveryRootWithItsOwnSubtree() {
    var w = TraceJoin.correlate("T",
        List.of(span("fe", "A", "", 10L), span("feKid", "B", "A", 20L), span("be", "C", "", 30L)),
        List.of());
    assertEquals("SPAN:fe\n  SPAN:feKid\nSPAN:be\n", shape(w));
  }

  // Rules 9-10
  @Test
  public void placesAnOrphanLogAndASpanlessLogUnderTheSyntheticRootAndCountsThem() {
    var w = TraceJoin.correlate("T",
        List.of(span("s", "A", "", 10L)),
        List.of(log("orphan", "ZZ", 20L), log("nospan", null, 30L)));
    assertEquals("SPAN:s\nUNATTACHED:" + Waterfall.UNATTACHED_ROOT_ID + "\n  LOG:orphan\n  LOG:nospan\n", shape(w));
    assertEquals(2, w.unattachedLogCount());
    assertEquals(2, w.logCount());
  }

  // Rule 9b
  @Test
  public void returnsARingOfSpansThatNameEachOtherRatherThanLosingIt() {
    var w = TraceJoin.correlate("T",
        List.of(span("a", "A", "B", 10L), span("b", "B", "A", 20L)),
        List.of(log("l", "A", 30L)));
    assertEquals("UNATTACHED:" + Waterfall.UNATTACHED_ROOT_ID + "\n  SPAN:a\n  SPAN:b\n  LOG:l\n", shape(w));
    assertEquals(2, w.unreachableSpanCount());
    assertEquals(1, w.unattachedLogCount());
  }

  // Rule 11
  @Test
  public void returnsALogsOnlyTraceRatherThanAnEmptyAnswer() {
    var w = TraceJoin.correlate("T", List.of(), List.of(log("l1", "A", 10L), log("l2", "B", 20L)));
    assertEquals("UNATTACHED:" + Waterfall.UNATTACHED_ROOT_ID + "\n  LOG:l1\n  LOG:l2\n", shape(w));
    assertEquals(2, w.unattachedLogCount());
    assertEquals(0, w.spanCount());
  }

  // Rule 12
  @Test
  public void omitsTheSyntheticRootWhenThereIsNothingToPutUnderIt() {
    var w = TraceJoin.correlate("T", List.of(span("s", "A", "", 10L)), List.of(log("l", "A", 20L)));
    assertFalse(shape(w).contains("UNATTACHED"));
    assertEquals(0, w.unattachedLogCount());
  }

  // Rules 13-14
  @Test
  public void keepsTheFirstReportOfASpanIdAndCountsTheDuplicateWithoutShowingIt() {
    var w = TraceJoin.correlate("T",
        List.of(span("first", "A", "", 10L), span("dupe", "A", "", 20L), span("kid", "B", "A", 30L)),
        List.of());
    assertEquals("SPAN:first\n  SPAN:kid\n", shape(w));
    assertEquals(1, w.duplicateSpanCount());
    assertEquals(3, w.spanCount(), "the duplicate is still a span that was reported");
  }

  @Test
  public void countsADuplicateEvenWhenItArrivesFirstInTheInputList() {
    var w = TraceJoin.correlate("T",
        List.of(span("dupe", "A", "", 20L), span("first", "A", "", 10L)),
        List.of());
    assertEquals("SPAN:first\n", shape(w), "first by timestamp wins, not first in the list");
    assertEquals(1, w.duplicateSpanCount());
  }

  @Test
  public void nestsChildrenOfTheSameParentAmongThemselvesInTimestampOrder() {
    var w = TraceJoin.correlate("T",
        List.of(span("root", "A", "", 10L), span("second", "C", "A", 40L), span("firstKid", "B", "A", 20L)),
        List.of());
    assertEquals("SPAN:root\n  SPAN:firstKid\n  SPAN:second\n", shape(w));
  }

  @Test
  public void returnsTheNestedTreeAndTheFlattenedListDescribingTheSameShape() {
    var w = TraceJoin.correlate("T",
        List.of(span("root", "A", "", 10L), span("kid", "B", "A", 20L)),
        List.of());
    assertEquals(1, w.roots().size());
    WaterfallNode root = w.roots().get(0);
    assertEquals("root", root.rowId());
    assertEquals(1, root.children().size());
    assertEquals("kid", root.children().get(0).rowId());
    assertTrue(root.children().get(0).children().isEmpty());
  }
}
