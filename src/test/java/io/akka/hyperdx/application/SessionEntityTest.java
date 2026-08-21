package io.akka.hyperdx.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.hyperdx.domain.Replay;
import io.akka.hyperdx.domain.ReplayChunk;
import io.akka.hyperdx.domain.SessionEvent;
import io.akka.hyperdx.domain.SessionState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** What the session entity adds on top of {@link ChunkReassembler}: a ceiling on held bodies. */
public class SessionEntityTest {

  private static final String BODY = "{\"type\":2,\"timestamp\":1700000000000}";

  @Test
  public void ignoresARedeliveredChunkRatherThanHoldingItTwice() {
    var kit = EventSourcedTestKit.<SessionState, SessionEvent, SessionEntity>of("S", SessionEntity::new);
    var c = new ReplayChunk("S", "e1", 0, 0, BODY, 100L);
    kit.method(SessionEntity::reportChunks).invoke(List.of(c));
    kit.method(SessionEntity::reportChunks).invoke(List.of(c));

    assertEquals(1, kit.getState().chunks().size());
    Replay r = kit.method(SessionEntity::replay).invoke().getReply();
    assertEquals(1, r.events().size());
  }

  @Test
  public void refusesChunksThatWouldTakeTheSessionPastItsBodyCeiling() {
    var kit = EventSourcedTestKit.<SessionState, SessionEvent, SessionEntity>of("S", SessionEntity::new);
    var big = "x".repeat(SessionEntity.MAX_BODY_BYTES);
    kit.method(SessionEntity::reportChunks).invoke(List.of(new ReplayChunk("S", "e1", 0, 0, big, 100L)));

    var refused = kit.method(SessionEntity::reportChunks)
        .invoke(List.of(new ReplayChunk("S", "e2", 0, 0, BODY, 200L)));
    assertTrue(refused.isError());
    assertTrue(refused.getError().contains(String.valueOf(SessionEntity.MAX_BODY_BYTES)),
        "the message names the ceiling: " + refused.getError());
    assertEquals(1, kit.getState().chunks().size());
  }
}
