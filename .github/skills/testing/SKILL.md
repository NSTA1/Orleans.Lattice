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
- **The tiered run strategy** - Tier 1 (single method) through Tier 4 (pre-PR), and the rule that the pre-PR run is **scoped to the fixtures your change can plausibly break** within the test project(s) covering the packages you changed, never the whole solution (on every PR CI re-runs the suites of every package the change can reach - the changed packages, every package that project-references them, and `lattice.dashboards` - so repeating that locally buys only wall-clock).
- **Categorization conventions** - which fixtures get `[Category("Integration")]` / `Chaos` / `AzureStorageEmulator` / `Docs` / `Coyote` / `Tlc` / `UI`.
- **Starting Azurite, and the false-green trap** - emulator-gated fixtures call `Assert.Inconclusive` when Azurite is unreachable, and NUnit counts that as neither passed, failed, nor skipped. A run missing 89 tests still prints `Passed!` with `Skipped: 0`, so the only signal is a lower `Total`. The master file has the `docker run` command and the affected projects.
- **False greens** - a green check that never exercised the property it names, which is worse than a red because it also asserts there is nothing to fix. The master file covers the seven shapes that have cost real time here, among them **reflection past the public seam** (a `BindingFlags.NonPublic` + `Invoke` fixture proves the member works when called and nothing about whether anything calls it - judge the *caller*, not the access modifier), and **restoring a perturbed file with `Copy-Item`** (it writes back the original `LastWriteTime`, so MSBuild skips the rebuild and keeps the perturbed binary).
- **The repository hygiene gates** - em-dash, mojibake, deletion-mandate, integration-category (and its per-project enrolment gate), serializable-exception deep-copy enrolment, UI-category, docs-snippet, performance-marker, and duplicate-XML-summary gates, what each enforces, and how to stay green.

Open that file and follow it directly.
