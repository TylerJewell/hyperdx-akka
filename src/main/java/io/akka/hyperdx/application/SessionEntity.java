package io.akka.hyperdx.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.SessionEvent;
import io.akka.hyperdx.domain.SessionState;
import java.util.List;

/**
 * One browser session: the replay chunks reported for it, and the trace ids observed under
 * it by the {@code rum.sessionId} attribute.
 *
 * <p>Chunks are kept as reported and reassembled at read time, for the same reason
 * {@link TraceEntity} joins at read time: whether an event is complete is a question about
 * the whole set, and answering it as each chunk lands would answer it too early.
 *
 * <p>A trace can be observed before or after the session has any chunks, which is rule 22 —
 * the entity has no notion of a session having "started".
 */
@Component(id = "session")
public class SessionEntity extends EventSourcedEntity<SessionState, SessionEvent> {

  /**
   * The ceiling on chunk bodies held for one session. Chunk bodies are the one part of this
   * model that is a text blob rather than a field, and a session replay grows for as long
   * as somebody keeps the tab open, so it is the state most likely to reach the megabyte
   * above which state stops replicating across regions. Half a megabyte leaves room for the
   * rest of the state and for the encoding overhead the raw character count does not see.
   */
  public static final int MAX_BODY_BYTES = 512_000;

  private final String sessionId;

  public SessionEntity(EventSourcedEntityContext context) {
    this.sessionId = context.entityId();
  }

  @Override
  public SessionState emptyState() {
    return SessionState.empty();
  }

  public Effect<Done> reportChunks(List<ReplayChunk> chunks) {
    if (chunks == null || chunks.isEmpty()) {
      return effects().reply(Done.getInstance());
    }
    int offered = chunks.stream().mapToInt(c -> c.body().length()).sum();
    if (currentState().bodyBytes() + offered > MAX_BODY_BYTES) {
      return effects().error("session holds " + currentState().bodyBytes()
          + " bytes of replay body and " + offered + " more were offered; the ceiling is "
          + MAX_BODY_BYTES);
    }
    return effects().persist(new SessionEvent.ChunksReported(chunks)).thenReply(s -> Done.getInstance());
  }

  public Effect<Done> observeTrace(String traceId) {
    if (currentState().traceIds().contains(traceId)) {
      return effects().reply(Done.getInstance());
    }
    return effects().persist(new SessionEvent.TraceObserved(traceId)).thenReply(s -> Done.getInstance());
  }

  public ReadOnlyEffect<Replay> replay() {
    var s = currentState();
    return effects().reply(ChunkReassembler.reassemble(sessionId, s.chunks(), s.traceIds()));
  }

  @Override
  public SessionState applyEvent(SessionEvent event) {
    return switch (event) {
      case SessionEvent.ChunksReported e -> currentState().withChunks(sessionId, e.chunks());
      case SessionEvent.TraceObserved e -> currentState().withTrace(sessionId, e.traceId());
    };
  }
}
