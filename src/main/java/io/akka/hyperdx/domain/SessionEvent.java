package io.akka.hyperdx.domain;

import akka.javasdk.annotations.TypeName;
import java.util.List;

/** What a session has been told, in the order it was told it. */
public sealed interface SessionEvent {

  @TypeName("chunks-reported")
  record ChunksReported(List<ReplayChunk> chunks) implements SessionEvent {}

  /** A trace observed under this session, by the `rum.sessionId` attribute (rule 21). */
  @TypeName("trace-observed")
  record TraceObserved(String traceId) implements SessionEvent {}
}
