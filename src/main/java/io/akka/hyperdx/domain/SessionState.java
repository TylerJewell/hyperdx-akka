package io.akka.hyperdx.domain;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Every chunk reported for one session, and every trace observed under it.
 *
 * <p>A chunk is identified by its event key and its index, so a redelivered chunk is
 * ignored rather than stored twice — it would reassemble to the same body either way, but a
 * retrying reporter should not be able to grow the state without bound.
 */
public record SessionState(
    String sessionId, List<ReplayChunk> chunks, List<String> traceIds, Set<String> chunkKeys) {

  public static SessionState empty() {
    return new SessionState(null, List.of(), List.of(), Set.of());
  }

  /** The bytes of chunk bodies held, which is what the state-size ceiling is about. */
  public int bodyBytes() {
    int total = 0;
    for (var c : chunks) total += c.body().length();
    return total;
  }

  public SessionState withChunks(String id, List<ReplayChunk> more) {
    var seen = new LinkedHashSet<>(chunkKeys);
    var next = new ArrayList<>(chunks);
    for (var c : more) {
      if (seen.add(c.eventKey() + "#" + c.position())) next.add(c);
    }
    return new SessionState(
        sessionId == null ? id : sessionId, List.copyOf(next), traceIds, Set.copyOf(seen));
  }

  public SessionState withTrace(String id, String traceId) {
    if (traceIds.contains(traceId)) {
      return this;
    }
    var next = new ArrayList<>(traceIds);
    next.add(traceId);
    return new SessionState(
        sessionId == null ? id : sessionId, chunks, List.copyOf(next), chunkKeys);
  }
}
