---
name: testing
description: Orleans.Lattice testing policy and the repository hygiene gates. Use when writing or running tests, choosing a test scope or tier, categorizing a fixture, or diagnosing or avoiding a CI hygiene-gate failure (em-dash, mojibake, integration-category, docs-snippet, or performance-marker gates).
---

# Testing

All Orleans.Lattice testing rules live in a single master file:

> **[`.github/instructions/testing.instructions.md`](../../instructions/testing.instructions.md)**

That file is the authority for everything this skill covers - do not restate its rules here or elsewhere; link to it so there is one place to change and nothing to drift. It covers:

- **Coverage policy and framework conventions** - the coverage rule, the test framework and mocking library, test naming, unit vs integration fixtures, assertions, file organization, and the shared test helpers to reuse instead of writing a private copy.
- **The tiered run strategy** - Tier 1 (while editing) through Tier 4 (before opening a PR): how to scope each run, and what CI re-runs on every PR, so a local run does not repeat it.
- **Starting Azurite, and the false-green trap** - emulator-gated fixtures fall through to `Assert.Inconclusive` when Azurite is unreachable, which NUnit counts as neither passed, failed, nor skipped, so a run without Azurite still reads as a pass. The master file has the `docker run` command and the affected projects.
- **The repository-wide gates** - the fixtures that scan every package whichever test project they live in, which a per-package run cannot see, and the `tools/Invoke-RepositoryWideGates.ps1` runner that executes them.
- **Categorization conventions** - which `[Category(...)]` each kind of fixture carries.
- **False greens** - the shapes of a green check that never exercised the property it names, which is worse than a red because it also asserts there is nothing to fix, and how to avoid each.
- **The Coyote concurrency tier, the browser UI tier, and the TLA+ specification** - what each verifies and how it runs.
- **The repository hygiene gates** - what each enforces, how to stay green, and how the gates reach CI.

Open that file and follow it directly.
