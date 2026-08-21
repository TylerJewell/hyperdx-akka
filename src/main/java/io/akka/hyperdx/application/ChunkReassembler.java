package io.akka.hyperdx.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.ReplayEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Session-replay chunks, in any order, into the events they were split from.
 * SPEC-001 §3 rules 15-20.
 *
 * <p>Chunks are addressed by their own index rather than by the order they were read in,
 * and an event is emitted only once every index it declares is present. That is decision 2,
 * and the difference it makes is the difference between a replay that is missing three
 * seconds and one that says so.
 *
 * <p>Pure and static, for the same reason {@link TraceJoin} is.
 */
public final class ChunkReassembler {

  private ChunkReassembler() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static Replay reassemble(String sessionId, Collection<ReplayChunk> chunks, List<String> traceIds) {
    // Index-addressed, and sorted by index, so concatenation order is the declared order
    // rather than the arrival order (rule 15). A repeated chunk overwrites its own slot and
    // so cannot be appended twice.
    Map<String, TreeMap<Integer, String>> bodies = new HashMap<>();
    Map<String, Integer> expected = new HashMap<>();
    Map<String, Long> firstChunkTimestamp = new HashMap<>();

    for (var c : chunks) {
      bodies.computeIfAbsent(c.eventKey(), k -> new TreeMap<>()).put(c.position(), c.body());
      expected.put(c.eventKey(), c.expectedChunks());
      // Rule 20: the first chunk's timestamp, which is a property of the event rather than
      // of when the pieces happened to be read.
      firstChunkTimestamp.merge(c.eventKey(), c.timestampNanos(), Math::min);
    }

    var events = new ArrayList<ReplayEvent>();
    var incomplete = new ArrayList<Replay.Incomplete>();
    var unparseable = new ArrayList<String>();

    for (var entry : bodies.entrySet()) {
      var eventKey = entry.getKey();
      var parts = entry.getValue();
      int total = expected.get(eventKey);

      var missing = new ArrayList<Integer>();
      for (int i = 1; i <= total; i++) {
        if (!parts.containsKey(i)) missing.add(i);
      }
      if (!missing.isEmpty()) {
        incomplete.add(new Replay.Incomplete(eventKey, total, List.copyOf(missing))); // Rule 17
        continue;
      }

      var body = String.join("", parts.values());
      try {
        MAPPER.readTree(body);
      } catch (Exception e) {
        // Rule 19: named, and only this event is affected — the events around it are
        // reassembled from their own slots and never shared an accumulator with it.
        unparseable.add(eventKey);
        continue;
      }
      events.add(new ReplayEvent(eventKey, firstChunkTimestamp.get(eventKey), body));
    }

    events.sort(Comparator.comparingLong(ReplayEvent::timestampNanos).thenComparing(ReplayEvent::eventKey));
    incomplete.sort(Comparator.comparing(Replay.Incomplete::eventKey));
    unparseable.sort(Comparator.naturalOrder());

    return new Replay(sessionId, List.copyOf(events), List.copyOf(incomplete),
        List.copyOf(unparseable), traceIds.stream().sorted().distinct().toList());
  }
}
