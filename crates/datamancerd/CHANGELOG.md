# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1](https://github.com/VoidstarSolutions/datamancer/compare/v0.5.0...v0.5.1) - 2026-07-18

### Added

- *(windows)* daemon compiles on Windows (real lock+signals, fail-closed control stub)
- *(windows)* Phase 1 cleanups + open-sourcing spec consolidation

### Fixed

- *(windows)* address CodeRabbit review on #35
- *(windows)* audit cleanup -- fail-closed pipe, byte-exact logs, native CI guard
- *(windows)* address review — defer admin-socket fallback, fence lang

### Other

- *(windows)* complete the credential-backend name contract
