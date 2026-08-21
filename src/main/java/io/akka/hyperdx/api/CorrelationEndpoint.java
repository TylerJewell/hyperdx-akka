package io.akka.hyperdx.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.CommandException;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpResponses;
import io.akka.hyperdx.application.SessionEntity;
import io.akka.hyperdx.application.TraceEntity;
import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.Waterfall;
import java.util.List;
import java.util.function.Supplier;

/**
 * The reachable surface for the correlation: report rows, ask for the joined answer.
 *
 * <p>Reporting spans is also where the session correlation happens — a span carrying a
 * {@code sessionId} makes its trace retrievable from that session (SPEC-001 rules 21-22).
 * It is done here rather than inside {@link TraceEntity} because it crosses two entities,
 * and an entity does not reach outside itself.
 *
 * <p>On size: question-log row 1 established by running it that one command may carry
 * 1,048,479 bytes of payload and metadata, and that going past it raises an
 * {@code IllegalArgumentException} naming the limit, so a caller batching too much in one
 * call is told so by the runtime. The per-trace and per-session ceilings, which the runtime
 * does not enforce, are the entities' own and reach a caller as a 400 through
 * {@link #reportingFull}.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/traces")
public class CorrelationEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;

  public CorrelationEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post("/{traceId}/spans")
  public HttpResponse reportSpans(String traceId, List<Span> spans) {
    return reportingFull(() -> {
      componentClient.forEventSourcedEntity(traceId)
          .method(TraceEntity::reportSpans)
          .invoke(spans);

      for (var sessionId : componentClient.forEventSourcedEntity(traceId)
          .method(TraceEntity::sessionIds).invoke()) {
        componentClient.forEventSourcedEntity(sessionId)
            .method(SessionEntity::observeTrace)
            .invoke(traceId);
      }
      return HttpResponses.ok();
    });
  }

  @Post("/{traceId}/logs")
  public HttpResponse reportLogs(String traceId, List<LogRecord> logs) {
    return reportingFull(() -> {
      componentClient.forEventSourcedEntity(traceId)
          .method(TraceEntity::reportLogs)
          .invoke(logs);
      return HttpResponses.ok();
    });
  }

  @Get("/{traceId}/waterfall")
  public Waterfall waterfall(String traceId) {
    return componentClient.forEventSourcedEntity(traceId).method(TraceEntity::waterfall).invoke();
  }

  @Post("/sessions/{sessionId}/chunks")
  public HttpResponse reportChunks(String sessionId, List<ReplayChunk> chunks) {
    return reportingFull(() -> {
      componentClient.forEventSourcedEntity(sessionId)
          .method(SessionEntity::reportChunks)
          .invoke(chunks);
      return HttpResponses.ok();
    });
  }

  @Get("/sessions/{sessionId}/replay")
  public Replay replay(String sessionId) {
    return componentClient.forEventSourcedEntity(sessionId).method(SessionEntity::replay).invoke();
  }

  /**
   * A full trace or session is a condition of the caller's own making, so it reaches them as
   * a 400 carrying the entity's message — which names the ceiling and the counts, and no row
   * content.
   */
  private static HttpResponse reportingFull(Supplier<HttpResponse> report) {
    try {
      return report.get();
    } catch (CommandException e) {
      return HttpResponses.badRequest(e.getMessage());
    }
  }
}
