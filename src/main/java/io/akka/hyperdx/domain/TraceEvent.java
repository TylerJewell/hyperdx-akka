package io.akka.hyperdx.domain;

import akka.javasdk.annotations.TypeName;
import java.util.List;

/** What a trace has been told, in the order it was told it. */
public sealed interface TraceEvent {

  @TypeName("spans-reported")
  record SpansReported(List<Span> spans) implements TraceEvent {}

  @TypeName("logs-reported")
  record LogsReported(List<LogRecord> logs) implements TraceEvent {}
}
