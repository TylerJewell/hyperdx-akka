package io.akka.hyperdx.domain;

import java.util.List;

/**
 * The reassembled answer for one session, SPEC-001 rules 15-20.
 *
 * <p>{@code incomplete} and {@code unparseable} are the point of decision 2: a replay
 * missing three seconds must not read the same as a replay in which nothing happened, so
 * what could not be reassembled is named here rather than omitted.
 */
public record Replay(
    String sessionId,
    List<ReplayEvent> events,
    List<Incomplete> incomplete,
    List<String> unparseable,
    List<String> traceIds) {

  /** An event some of whose chunks never arrived, named with the indices still missing. */
  public record Incomplete(String eventKey, int totalChunks, List<Integer> missingIndices) {}
}
