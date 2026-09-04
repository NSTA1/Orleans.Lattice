---
name: testing
description: Orleans.Lattice testing policy and the repository hygiene gates. Use when writing or running tests, choosing a test scope or tier, categorizing a fixture, or diagnosing or avoiding a CI hygiene-gate failure (em-dash, mojibake, integration-category, docs-snippet, or performance-marker gates).
---

# Testing

All Orleans.Lattice testing rules live in a single master file:

> **[`.github/instructions/testing.instructions.md`](../../instructions/testing.instructions.md)**

That file is the authority for everything this skill covers - do not restate its rules here or elsewhere; link to it so there is one place to change and nothing to drift. It contains:

- **Coverage policy** - every public type and member needs at least one test.
- **Framework and conventions** - NUnit 4.x / NSubstitute, test naming, unit vs integration fixtures, assertions, file organization.
- **The tiered run strategy** - Tier 1 (single method) through Tier 4 (pre-PR), and the rule that the pre-PR run is **scoped to the fixtures your change can plausibly break** within the test project(s) covering the packages you changed, never the whole solution (CI re-runs the full per-package and cross-solution non-chaos sweep on every PR, so repeating it locally buys only wall-clock).
- **Categorization conventions** - which fixtures get `[Category("Integration")]` / `Chaos` / `AzureStorageEmulator` / `Docs`.
- **Starting Azurite, and the false-green trap** - emulator-gated fixtures call `Assert.Inconclusive` when Azurite is unreachable, and NUnit counts that as neither passed, failed, nor skipped. A run missing 89 tests still prints `Passed!` with `Skipped: 0`, so the only signal is a lower `Total`. The master file has the `docker run` command and the affected projects.
- **The repository hygiene gates** - em-dash, mojibake, deletion-mandate, integration-category, docs-snippet, and performance-marker gates, what each enforces, and how to stay green.

Open that file and follow it directly.
