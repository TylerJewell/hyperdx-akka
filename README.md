# hyperdx-akka

Takes the timing records, log lines and screen recordings a running system reports, and
works out which belongs under which, in what order.

A port of [hyperdxio/hyperdx](https://github.com/hyperdxio/hyperdx) onto **Akka**, built
with **Akka Specify**.

---

## Where it came from

hyperdxio/hyperdx is a tool for looking at what a running system did: it collects the
timing records a program writes as it works, the log lines it prints, and a recording of
what a person saw on screen, and shows them together. It was ported to derive a
specification format precise enough to regenerate a system on a different stack — the port
is the vehicle, the specification is the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `hyperdx-port/`.

---

## hyperdxio/hyperdx → this port

📉 157 TypeScript lines → **159 Java lines**<br>
📁 2 files → **2 files**<br>
🧪 0 tests → **33 tests**<br>
⚡ 2,414,725 → **173,999** nanoseconds, largest workload<br>
⚡ 12,366 → **2,114** nanoseconds, smallest workload<br>
🎯 6 of 12 → **6 of 12** workloads answered identically<br>
🔁 2 → **1** answers given for the same six records delivered six ways

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/hyperdx-port/bench/REPORT.md).

---

## What it took to build

⏱️ **0.8 hours** from the first command to the published repository, **0.8** of them active<br>
💬 **275** exchanges with the model<br>
✍️ **232,197** tokens written by the model, **48,405,871** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **34** tests

```bash
python toolkit/tokens.py --port hyperdx    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

- **A record can arrive before the record it belongs under, and still ends up under it.**
  A piece of work reports itself at a different moment from the work that caused it, and
  the two often reach here in the wrong order.
- **The same records always give the same answer, whatever order they arrived in.** The
  answer is worked out from the records themselves rather than from the sequence they were
  handed over in.
- **Nothing is dropped without saying so.** A log line whose piece of work was never
  reported, a screen recording missing a fragment, a record reported twice — each is
  returned or counted, never quietly left out.
- **Two records stamped with the same instant have a settled order.** The one describing a
  piece of work comes before the one describing a log line, and after that they go by
  their own identifiers.
- **A screen recording split into fragments is put back together by fragment number.**
  Fragments that arrive out of order reassemble; one that never arrives is reported by
  number rather than losing the whole moment.

---

## Design decisions

**Work it out when asked, not when told.** Where a record belongs depends on records that
may not have arrived yet, so deciding as each one lands would decide too early. Everything
is kept as reported and the answer is built at the moment somebody asks for it.

**One holder per group of records.** Every record about the same unit of work goes to the
same holder, so a record put in and an answer asked for a moment later always see each
other. Nothing has to wait for a copy to catch up.

**Fragments carry their own number.** A recording split into pieces is put back together by
the number each piece carries rather than by the order the pieces were read. The order they
were read in is not something anybody controls.

**Say what is missing.** A recording that is missing a fragment is returned along with the
number of the fragment that never came. A recording missing five seconds and a recording of
five seconds in which nothing happened are not the same thing, and a reader has to be able
to tell them apart.

**A ceiling that says so.** Each group of records stops accepting new ones past a fixed
size, and refuses with a message naming the size. There is no natural end to how many
records a long-running piece of work can report, and quietly forgetting the older ones
would be the same fault this port exists to fix.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/hyperdx-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Try it** against http://localhost:9029 — there is no page to open; the five addresses
below are the whole of it.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9029**.

### The five addresses

| | |
|---|---|
| `POST /traces/{traceId}/spans` | report units of work |
| `POST /traces/{traceId}/logs` | report log lines |
| `GET /traces/{traceId}/waterfall` | ask for the correlated answer |
| `POST /traces/sessions/{sessionId}/chunks` | report fragments of a screen recording |
| `GET /traces/sessions/{sessionId}/replay` | ask for the reassembled recording |

```bash
curl -X POST localhost:9029/traces/t1/spans -H 'Content-Type: application/json' -d '[
  {"rowId":"b","traceId":"t1","spanId":"B","parentSpanId":"A","timestampNanos":10,
   "durationNanos":5,"serviceName":"api","spanName":"query","statusCode":"Ok","sessionId":null}]'

curl -X POST localhost:9029/traces/t1/spans -H 'Content-Type: application/json' -d '[
  {"rowId":"a","traceId":"t1","spanId":"A","parentSpanId":"","timestampNanos":20,
   "durationNanos":9,"serviceName":"web","spanName":"GET /","statusCode":"Ok","sessionId":null}]'

curl localhost:9029/traces/t1/waterfall
```

The child was reported first and comes back nested under the parent.

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | Nothing about this service is configured from the environment. The two ceilings are constants in the code: 10,000 records for one unit of work, 512,000 characters of screen recording for one session. |

There is no model provider. This port calls no model.

---

## Where it differs from hyperdxio/hyperdx

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **A log line whose piece of work was never reported.** hyperdxio/hyperdx does not show
  it, and reports no count of what it left out. This port returns it under a single extra
  box at the bottom, together with a count, because most work is sampled away before it is
  ever reported while its log lines still arrive — so this is the ordinary case rather than
  an unusual one, and an answer of "nothing here" from a system that is holding something
  is a wrong answer.
- **A group made only of log lines.** hyperdxio/hyperdx returns nothing at all. This port
  returns the log lines, for the same reason as above.
- **A log line reported without naming any piece of work.** hyperdxio/hyperdx does not show
  it. This port returns it under the same extra box.
- **A piece of work reported twice under the same identifier.** hyperdxio/hyperdx shows both
  copies, the second one on its own with nothing under it. This port shows the first and
  reports the second as a count, because a lone second copy reads to a person as a real
  second piece of work, and a count reads as what it is.
- **Two records stamped with the exact same instant.** In hyperdxio/hyperdx the order
  between them is whatever the order they were handed over in left behind — six deliveries
  of the same six records produced two different answers, measured. This port fixes the
  order: work before log lines, then by identifier. The order it fixes on is the one
  hyperdxio/hyperdx in fact produces, so nothing visible today moves.
- **A screen recording whose fragments arrive out of order.** hyperdxio/hyperdx joins them
  in the order they arrived and loses the moment when that is not the order they were split
  in, without recording that anything was lost. This port joins them by the number each
  fragment carries, so the order they arrived in does not matter.
- **A screen recording missing a fragment.** hyperdxio/hyperdx loses the moment silently.
  This port returns the recording without it and names the number of the fragment that
  never came, so a gap can be told apart from a quiet stretch.
- **A moment whose text does not make sense once rejoined.** Both leave the rest of the
  recording alone. This port also names the moment; hyperdxio/hyperdx does not.
- **Pieces of work that name each other as their cause, in a ring.** Neither system can put
  a ring under anything, so hyperdxio/hyperdx shows none of them. This port returns them
  under the extra box with a count. Whether hyperdxio/hyperdx can be made to produce this
  at all was **not checked** — it is reachable here because records arrive one call at a
  time.
- **Where the records come from.** hyperdxio/hyperdx reads them out of a database somebody
  else runs and holds none of them. This port is told them and holds them itself, which is
  why it has addresses for reporting records and hyperdxio/hyperdx does not.
- **A ceiling on how much is held.** hyperdxio/hyperdx has none, because the database it
  reads from has none. This port refuses records past 10,000 for one unit of work and past
  512,000 characters of screen recording for one session, with a message naming the
  ceiling. Beyond roughly a million characters what it holds would stop being copied
  between regions, and stopping loudly below that is better than stopping silently above it.
- **The same record reported twice by a reporter that retried.** hyperdxio/hyperdx reads
  from a database and does not see a retry. This port ignores the second copy, because
  counting it would report a fault in the watched system when the fault is in the delivery.
- **How long the answer takes.** Measured on the same records, this port is between 1.8
  times slower and 13.9 times faster depending on the case, and the whole comparison is in
  [`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/hyperdx-port/bench/REPORT.md).
  Neither side's figure includes fetching the records, which in hyperdxio/hyperdx is most
  of the real cost.
- **Everything hyperdxio/hyperdx does that is not this.** Alerts, dashboards, charts,
  metrics, the map of which service calls which, the collector, the command-line tool and
  the settings screens are all absent. That is scope, not a difference in behaviour.

---

## Licence

hyperdxio/hyperdx is MIT, © 2023 DeploySentinel, Inc. This port reimplements the behaviour
without copied source and is MIT itself; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
