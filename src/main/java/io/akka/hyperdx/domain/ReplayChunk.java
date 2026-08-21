package io.akka.hyperdx.domain;

/**
 * One piece of one session-replay event, SPEC-001 §2.
 *
 * <p>{@code chunkIndex} is 1-based. Both it and {@code totalChunks} being {@code 0} means
 * the event was never split, which rule 18 treats as a single complete chunk.
 */
public record ReplayChunk(
    String sessionId,
    String eventKey,
    int chunkIndex,
    int totalChunks,
    String body,
    long timestampNanos) {

  public boolean unchunked() {
    return chunkIndex == 0 && totalChunks == 0;
  }

  /** How many chunks this event is made of, counting an unchunked event as one. */
  public int expectedChunks() {
    return unchunked() ? 1 : totalChunks;
  }

  /** This chunk's 1-based position, counting an unchunked event's only chunk as 1. */
  public int position() {
    return unchunked() ? 1 : chunkIndex;
  }
}
