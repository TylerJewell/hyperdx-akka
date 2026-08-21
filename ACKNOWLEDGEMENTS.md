# Acknowledgements

This project is a port of **[hyperdxio/hyperdx](https://github.com/hyperdxio/hyperdx)**.

## Licence and copyright

hyperdxio/hyperdx is under the **MIT Licence**, © 2023 DeploySentinel, Inc. Read from the
repository's own `LICENSE` file, not from a badge.

## Was anything copied verbatim?

**Into this repository, no.** No file here contains source from hyperdxio/hyperdx. Every
Java file was written for this port.

**Into the harness, yes, and it is named.** Three routines — the row merge, the tree build,
and the session-replay chunk reassembler — were lifted verbatim into
`hyperdx-port/probes/source-probe/join.mjs` in the harness repository, so that the original
could be *run* rather than only read, and so that the two systems could be given the same
inputs and compared. That file carries hyperdx's MIT licence and says where each routine
came from. It is a probe; nothing in it is shipped here.

## What licence does that force on this project?

The MIT Licence, which is what this project is under. The behaviour is derived from
hyperdxio/hyperdx even where no text was copied, and MIT is the licence that came with it.
The copyright notice above is reproduced as MIT requires.

## Is behaviour derived even where no text was copied?

Yes, and that is the whole point of the exercise. The rules this port implements were
established by reading and running hyperdxio/hyperdx, and are written down with citations
in `hyperdx-port/specs/SPEC-001-hyperdx.md` in the harness repository. Where this port
decided differently, the README says so and says why.

## Also used

- [Akka](https://akka.io) — the platform this was rebuilt on.
- [timestamp-nano](https://www.npmjs.com/package/timestamp-nano) — used by the probe, not
  by this project; it is the library hyperdx's own comparator depends on, and installing
  the real one rather than imitating it is what made the timing comparison fair.
