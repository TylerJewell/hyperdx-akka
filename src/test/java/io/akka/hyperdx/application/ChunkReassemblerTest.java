package io.akka.hyperdx.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.ReplayEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 15-20. */
public class ChunkReassemblerTest {

  private static final String BODY = "{\"type\":2,\"timestamp\":1700000000000,\"data\":{\"x\":1}}";

  private static List<ReplayChunk> split(String eventKey, String body, int parts, long baseTs) {
    var out = new ArrayList<ReplayChunk>();
    int size = (body.length() + parts - 1) / parts;
    for (int i = 0; i < parts; i++) {
      var slice = body.substring(i * size, Math.min(body.length(), (i + 1) * size));
      out.add(new ReplayChunk("S", eventKey, i + 1, parts, slice, baseTs + i));
    }
    return out;
  }

  // Rules 15-16
  @Test
  public void reassemblesByIndexWhateverOrderTheChunksArriveIn() {
    var chunks = split("e1", BODY, 4, 100L);
    var expected = ChunkReassembler.reassemble("S", chunks, List.of());
    assertEquals(1, expected.events().size());
    assertEquals(BODY, expected.events().get(0).body());

    for (var permutation : permutations(chunks)) {
      var r = ChunkReassembler.reassemble("S", permutation, List.of());
      assertEquals(1, r.events().size(), "arrival order lost the event: " + indices(permutation));
      assertEquals(BODY, r.events().get(0).body(), "arrival order " + indices(permutation) + " reassembled wrongly");
      assertTrue(r.incomplete().isEmpty());
    }
  }

  private static List<Integer> indices(List<ReplayChunk> cs) {
    return cs.stream().map(ReplayChunk::chunkIndex).toList();
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

  // Rule 17
  @Test
  public void reportsAnIncompleteEventWithTheIndicesStillMissing() {
    var chunks = new ArrayList<>(split("e1", BODY, 4, 100L));
    chunks.remove(2); // index 3
    var r = ChunkReassembler.reassemble("S", chunks, List.of());
    assertEquals(List.of(), r.events());
    assertEquals(1, r.incomplete().size());
    assertEquals("e1", r.incomplete().get(0).eventKey());
    assertEquals(4, r.incomplete().get(0).totalChunks());
    assertEquals(List.of(3), r.incomplete().get(0).missingIndices());
  }

  @Test
  public void anIncompleteEventDoesNotCostTheEventsAroundIt() {
    var broken = new ArrayList<>(split("broken", BODY, 3, 100L));
    broken.remove(1);
    var whole = new ArrayList<>(broken);
    whole.addAll(split("whole", BODY, 2, 200L));
    var r = ChunkReassembler.reassemble("S", whole, List.of());
    assertEquals(1, r.events().size());
    assertEquals("whole", r.events().get(0).eventKey());
    assertEquals(1, r.incomplete().size());
  }

  // Rule 18
  @Test
  public void treatsAnUnchunkedEventAsOneCompleteChunk() {
    var r = ChunkReassembler.reassemble("S",
        List.of(new ReplayChunk("S", "e1", 0, 0, BODY, 100L)), List.of());
    assertEquals(1, r.events().size());
    assertEquals(BODY, r.events().get(0).body());
    assertTrue(r.incomplete().isEmpty());
  }

  // Rule 19
  @Test
  public void reportsAnUnparseableEventAndLeavesTheOthersAlone() {
    var bad = List.of(new ReplayChunk("S", "bad", 0, 0, "{not json", 100L));
    var good = split("good", BODY, 2, 200L);
    var all = new ArrayList<ReplayChunk>(bad);
    all.addAll(good);
    var r = ChunkReassembler.reassemble("S", all, List.of());
    assertEquals(List.of("bad"), r.unparseable());
    assertEquals(1, r.events().size());
    assertEquals("good", r.events().get(0).eventKey());
  }

  // Rule 20
  @Test
  public void ordersEmittedEventsByTheirFirstChunkTimestamp() {
    var all = new ArrayList<ReplayChunk>();
    all.addAll(split("later", BODY, 3, 900L));
    all.addAll(split("earlier", BODY, 3, 100L));
    var r = ChunkReassembler.reassemble("S", all, List.of());
    assertEquals(List.of("earlier", "later"), r.events().stream().map(ReplayEvent::eventKey).toList());
    assertEquals(100L, r.events().get(0).timestampNanos(),
        "the first chunk's timestamp, not whichever chunk arrived last");
  }

  @Test
  public void breaksAnIdenticalFirstChunkTimestampByEventKey() {
    var all = new ArrayList<ReplayChunk>();
    all.addAll(split("b", BODY, 1, 100L));
    all.addAll(split("a", BODY, 1, 100L));
    var r = ChunkReassembler.reassemble("S", all, List.of());
    assertEquals(List.of("a", "b"), r.events().stream().map(ReplayEvent::eventKey).toList());
  }

  @Test
  public void aRepeatedChunkIsNotAppendedTwice() {
    var chunks = new ArrayList<>(split("e1", BODY, 3, 100L));
    chunks.add(chunks.get(0));
    var r = ChunkReassembler.reassemble("S", chunks, List.of());
    assertEquals(1, r.events().size());
    assertEquals(BODY, r.events().get(0).body());
  }

  @Test
  public void carriesTheCorrelatedTraceIdsThrough() {
    var r = ChunkReassembler.reassemble("S", List.of(), List.of("t2", "t1"));
    assertEquals(List.of("t1", "t2"), r.traceIds(), "sorted, so the answer does not depend on arrival order");
  }
}
