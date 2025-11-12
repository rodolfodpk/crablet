# Crablet

[![Build Status](https://github.com/rodolfodpk/crablet/actions/workflows/ci.yml/badge.svg)](https://github.com/rodolfodpk/crablet/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rodolfodpk/crablet/branch/main/graph/badge.svg)](https://codecov.io/gh/rodolfodpk/crablet)

So far, just an experiment

## Requirements

- **Java**: 21 or higher
- **Maven**: 3.6+ (or use the included Maven wrapper `./mvnw`)
- **Kotlin**: 2.1.0
- **Docker**: Required for running tests (integration tests use Testcontainers to automatically spin up PostgreSQL containers)

## Testing

Before running tests, ensure Docker is running on your machine. Testcontainers will automatically start PostgreSQL containers as needed.

### Run all tests

```bash
./mvnw test verify
```

This will run:
- Unit tests (files ending with `Test.kt`)
- Integration tests (files ending with `IT.kt`)

### Run only unit tests

```bash
./mvnw test
```

### Run only integration tests

```bash
./mvnw verify
```

### Run a specific test class

```bash
./mvnw test -Dtest=AccountTransferScenarioIT
```

## Inspirations

* [The Aggregate is dead. Long live the Aggregate! - Sara Pellegrini & Milan Savić. - The main motivation for Crablet](https://www.youtube.com/watch?v=MpalYQaKKd4)
* [Event Sourcing You are doing it wrong by David Schmitz. - Very good advices. I will try events as plain JSON.](https://www.youtube.com/watch?v=GzrZworHpIk)
* [Json-values: I guess it's a perfect library for using events as plain JSON. Crablet is agnostic about it, but I will try it in Crablet examples.](https://github.com/imrafaelmerino/json-values)
  by [Rafael Merino](https://github.com/imrafaelmerino)
* [My previous event sourcing experiment: Crabzilla. I learned a lot but it ended as a framework. Crablet hopefully will be just a library.](https://github.com/crabzilla/crabzilla)