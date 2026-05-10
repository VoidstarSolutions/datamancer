# datamancer-core

Types and trait surface for datamancer's provider and storage backends. Provider crates depend on this without pulling in the orchestrator.

- No I/O orchestration here. This crate is pure types + traits.
- Keep dependencies minimal. New providers should depend on this crate alone, not on the `datamancer` orchestrator.
