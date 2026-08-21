package io.akka.hyperdx;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.hyperdx.application.ChunkReassembler;
import io.akka.hyperdx.application.TraceJoin;
import io.akka.hyperdx.domain.LogRecord;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.Span;
import io.akka.hyperdx.domain.Waterfall;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Runs the same workloads {@code hyperdx-port/bench/run_source.mjs} runs, and writes the
 * answer and the time per operation for each, for {@code hyperdx-port/bench/compare.py} to
 * put side by side.
 *
 * <p>It measures {@link TraceJoin} and {@link ChunkReassembler} directly, with no runtime
 * started and no entity in the way, because that is what the source side measures too — the
 * source's join runs in a browser with no store or transport around it either. A timing
 * that included the entity would be comparing this port's storage against nothing.
 */
public class BenchmarkHarnessTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Path BENCH = Path.of("..", "hyperdx-port", "bench");

  @Test
  public void writesAnAnswerAndATimingForEveryWorkload() throws IOException {
    var workloads = MAPPER.readTree(Files.readString(BENCH.resolve("workloads.json")));
    var out = MAPPER.createObjectNode();

    for (var w : workloads) {
      var name = w.get("name").asText();
      var sequence = w.has("sequence") ? w.get("sequence").asText() : "";
      Supplier<String> run;
      ObjectNode extra = MAPPER.createObjectNode();

      switch (sequence) {
        case "chunks" -> {
          var chunks = chunks(w.get("chunks"));
          run = () -> emitted(ChunkReassembler.reassemble("S", chunks, List.of()));
          var r = ChunkReassembler.reassemble("S", chunks, List.of());
          extra.put("incomplete", r.incomplete().stream()
              .map(i -> i.eventKey() + " missing " + i.missingIndices()).toList().toString());
          extra.put("unparseable", r.unparseable().toString());
        }
        case "arrival-orders" -> {
          var rows = rows(w.get("rows"));
          var orders = new ArrayList<List<Integer>>();
          for (var o : w.get("orders")) {
            var one = new ArrayList<Integer>();
            for (var i : o) one.add(i.asInt());
            orders.add(one);
          }
          run = () -> {
            var parts = new ArrayList<String>();
            for (var order : orders) {
              var permuted = order.stream().map(rows::get).toList();
              parts.add(shape(correlate(permuted)));
            }
            return String.join("\n--- next order ---\n", parts);
          };
        }
        case "cumulative" -> {
          var steps = new ArrayList<List<JsonNode>>();
          for (var s : w.get("steps")) steps.add(rows(s));
          run = () -> {
            var seen = new ArrayList<JsonNode>();
            var parts = new ArrayList<String>();
            for (var s : steps) {
              seen.addAll(s);
              parts.add(shape(correlate(seen)));
            }
            return String.join("\n--- next report ---\n", parts);
          };
        }
        default -> {
          var rows = rows(w.get("rows"));
          run = () -> shape(correlate(rows));
        }
      }

      int reps = name.equals("wide-with-logs") || name.equals("adoption") ? 200 : 2000;
      var node = MAPPER.createObjectNode();
      node.put("answer", run.get());
      node.put("nsPerOp", timeNanos(run, reps));
      if (!extra.isEmpty()) node.set("portReports", extra);
      out.set(name, node);
    }

    var target = BENCH.resolve("port-answers.json");
    Files.writeString(target, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(out));
    assertTrue(Files.exists(target));
  }

  private static List<JsonNode> rows(JsonNode array) {
    var out = new ArrayList<JsonNode>();
    array.forEach(out::add);
    return out;
  }

  private static List<ReplayChunk> chunks(JsonNode array) {
    var out = new ArrayList<ReplayChunk>();
    for (var c : array) {
      out.add(new ReplayChunk("S", c.get("eventKey").asText(), c.get("chunkIndex").asInt(),
          c.get("totalChunks").asInt(), c.get("body").asText(), c.get("timestampNanos").asLong()));
    }
    return out;
  }

  private static Waterfall correlate(List<JsonNode> rows) {
    var spans = new ArrayList<Span>();
    var logs = new ArrayList<LogRecord>();
    for (var r : rows) {
      var rowId = r.get("rowId").asText();
      long ts = r.get("timestampNanos").asLong();
      var spanId = r.hasNonNull("spanId") ? r.get("spanId").asText() : null;
      if (r.get("kind").asText().equals("span")) {
        var parent = r.hasNonNull("parentSpanId") ? r.get("parentSpanId").asText() : "";
        spans.add(new Span(rowId, "T", spanId, parent, ts, 1, "svc", spanId, "Ok", null));
      } else {
        logs.add(new LogRecord(rowId, "T", spanId, ts, "body", "info"));
      }
    }
    return TraceJoin.correlate("T", spans, logs);
  }

  private static String shape(Waterfall w) {
    var sb = new StringBuilder();
    for (var n : w.flattened()) {
      if (sb.length() > 0) sb.append('\n');
      sb.append("  ".repeat(n.level())).append(n.kind()).append(':').append(n.rowId());
    }
    return sb.toString();
  }

  private static String emitted(Replay r) {
    var sb = new StringBuilder();
    for (var e : r.events()) {
      if (sb.length() > 0) sb.append('\n');
      sb.append(e.body());
    }
    return sb.toString();
  }

  /** Warmed first, because the first call through a just-in-time compiler is not the one
   * being measured — the same allowance the source side is given. */
  private static long timeNanos(Supplier<String> run, int reps) {
    for (int i = 0; i < 2000; i++) run.get();
    long start = System.nanoTime();
    for (int i = 0; i < reps; i++) run.get();
    return (System.nanoTime() - start) / reps;
  }
}
