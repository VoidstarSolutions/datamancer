# Building datamancer natively on Windows

Native Windows support for the full stack (daemon + transport) is in progress —
see [`docs/superpowers/specs/2026-07-15-native-windows-support-design.md`](superpowers/specs/2026-07-15-native-windows-support-design.md)
(#29) for the architecture and phased plan.

## Toolchain requirement: LLVM / libclang

The `datamancer-transport-iceoryx2` crate depends on iceoryx2, whose build script
runs `bindgen`, which needs **libclang** at build time on every platform — Windows
included. Without it, a full-workspace build fails with:

```text
Unable to find libclang: "couldn't find any valid shared libraries matching:
['clang.dll', 'libclang.dll'], set the `LIBCLANG_PATH` environment variable ..."
```

### Install LLVM and point the build at it

```powershell
winget install LLVM.LLVM
# then set LIBCLANG_PATH to the LLVM bin dir (once, persisted):
setx LIBCLANG_PATH "C:\Program Files\LLVM\bin"
# (open a new shell so the variable is picked up)
```

An MSVC toolchain (Visual Studio Build Tools) is also required for linking — the
same one `rustc`'s `x86_64-pc-windows-msvc` target already uses.

## What builds vs. runs today

- **The ws-portable subset** (`datamancer-transport-ws` +
  `datamancer-client --features ws`) builds and tests on Windows with no extra
  toolchain — this is what Windows CI covers.
- **The library** (`cargo build -p datamancer`, default features) builds and runs
  on Windows. Scope example runs to the package to avoid pulling in the iceoryx2
  workspace member: `cargo run -p datamancer --example cached_history`.
- **`datamancer-transport-iceoryx2`** *compiles* on Windows once libclang is
  installed, but iceoryx2 **does not run** on Windows at the pinned version (its
  PAL reads the POSIX user database at node creation). The native-Windows data
  transport is therefore WS-over-loopback, not iceoryx2 — see the spec above.
