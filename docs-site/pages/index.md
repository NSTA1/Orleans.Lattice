---
title: Documentation
_layout: landing
---

<!--
DIRECTION CONTRACT - Orleans.Lattice documentation site
THESIS: The site is drawn in the notation the product is named after: states as nodes, order as edges, merges as joins. It refuses the category's hero-plus-feature-cards landing and the stock DocFX look.
OWN-WORLD: Ink on white paper by day, chalk on green-grey slate by night; one yellow marker reserved for the join and for "you are here"; Recursive Sans Linear for every voice, Cascadia Mono for code; hairline spines with node glyphs, booktabs tables.
STORY: A reader learns what Lattice is, watches two concurrent writes converge on one state, and takes the Build, Evaluate, or Operate chain to their answer.
FIRST VIEWPORT: Left, the thesis headline, a three-sentence lede, and the three ways in; right, the four-element lattice of a G-Counter merge at full height, replayable.
FORM: Order diagram, candidate 7 of 7, seed a5e4ebc3. Staging: one proportional family carries every voice, weight marks importance; code alone takes a second face.
-->

<div class="lt-home">
<section class="lt-hero" aria-labelledby="lt-hero-title">
<div class="lt-hero-copy">
<h1 id="lt-hero-title">State that lives in your Orleans cluster and converges without locks.</h1>
<p class="lt-lede">Orleans.Lattice is a sorted, durable, horizontally-scalable key-value store embedded in your Orleans cluster. Its conflict resolution is algebraic, so any cluster can accept a write to any key with no lock manager and no consensus round trip. Run it on one machine, then active-active across regions, with the same <code>ILattice</code> code.</p>
<nav class="lt-ways" aria-label="Ways into the documentation">
<a class="lt-way" href="#build"><span class="lt-way-name">Build</span><span class="lt-way-for">Writing code against <code>ILattice</code></span></a>
<a class="lt-way" href="#evaluate"><span class="lt-way-name">Evaluate</span><span class="lt-way-for">Deciding whether it fits</span></a>
<a class="lt-way" href="#operate"><span class="lt-way-name">Operate</span><span class="lt-way-for">Running a Lattice estate</span></a>
</nav>
</div>
<!-- lattice:join-figure -->
</section>
<section class="lt-paths" aria-labelledby="lt-paths-title">
<h2 id="lt-paths-title">Three ways in</h2>
<p class="lt-section-lede">Each way in is a chain of pages in reading order. Start at its first node, or join it wherever you already are.</p>
<div class="lt-path-grid">
<div class="lt-path" id="build">
<h3>Build</h3>
<p class="lt-path-for">You have chosen Orleans.Lattice and are writing code against <code>ILattice</code>.</p>

1. [Quick start](README.md#quick-start) Register Lattice on a silo, then read and write your first keys.
2. [API reference](docs/lattice/api.md) The public `ILattice` interface, batch operations, options, and serializable types.
3. [Configuration](docs/lattice/configuration.md) Options, per-tree overrides, immutability constraints, and the storage provider. <span class="lt-shared">Also on Operate</span>
4. [Predicate operations](docs/lattice/predicated-operations.md) Server-side filters for typed reads, conditional writes, scans, cursors, and range deletes.
5. [CRDT primitives](docs/crdt/readme.md) Counters, registers, sets, and maps that resolve concurrent writes by construction.
6. [Atomic writes](docs/lattice/atomic-writes.md) All-or-nothing batches across keys, shards, trees, and replicating clusters.
7. [Samples](samples/index.md) Runnable projects, grouped by concern.

</div>
<div class="lt-path" id="evaluate">
<h3>Evaluate</h3>
<p class="lt-path-for">You are deciding whether the platform fits, and want the evidence.</p>

1. [What it is and why it exists](README.md#what-is-it) The store, the problem it solves, and the three positions it takes.
2. [A core plus seams](README.md#architecture-a-core-plus-seams) How companion packages fill documented seams, and why one you leave out costs nothing.
3. [Consistency guarantees](docs/lattice/consistency.md) The contract for what a caller of `ILattice` observes, operation by operation.
4. [Chaos tests](docs/lattice/chaos-tests.md) A live cluster under concurrent load, topology changes, network partitions, and storage faults.
5. [Verified atomic commit](docs/lattice/verified-atomic-commit.md) The commit protocol's deterministic core, machine-checked with Coyote and a TLA+ specification.
6. [Single-silo performance](docs/lattice/performance-single-silo.md) Measured throughput and latency against real Azure Tables.
7. [Reference architecture](reference-architecture.md) An active-active, cross-region estate on Azure Container Apps, with a deployment kit.

</div>
<div class="lt-path" id="operate">
<h3>Operate</h3>
<p class="lt-path-for">You run a Lattice estate and need it sized, observed, and recoverable.</p>

1. [Configuration](docs/lattice/configuration.md) The options reference and per-tree overrides. <span class="lt-shared">Also on Build</span>
2. [Tree sizing](docs/lattice/tree-sizing.md) Resize a live tree's leaf and internal-node limits, with an undo window.
3. [WAL tuning](docs/lattice/wal-tuning.md) How the write-ahead log's concurrency limits meet a durable backend's throughput envelope.
4. [Metrics](docs/lattice/metrics.md) Runtime telemetry through `System.Diagnostics.Metrics`, for any OpenTelemetry exporter.
5. [Dashboards](docs/lattice.dashboards/README.md) Bundled Grafana dashboards for the Lattice meters.
6. [Troubleshooting](docs/lattice/troubleshooting.md) Symptom-driven diagnosis, starting from a `DiagnoseAsync` report.
7. [Disaster recovery](docs/lattice.backup/disaster-recovery.md) Recovering backups after losing the cluster that took them.
8. [Explorer console](docs/lattice.explorer/running-the-explorer.md) A read-only, auth-aware web console over the cluster's gRPC APIs. <span class="lt-status">in progress</span>

</div>
</div>
</section>
<section class="lt-journey" aria-labelledby="lt-journey-title">
<h2 id="lt-journey-title">One programming model, Local to Global</h2>
<p class="lt-section-lede">A deployment grows in three stages. The code that reads and writes data resolves <code>ILattice</code> and calls it in all three; each stage adds companion packages and configuration, not a rewrite.</p>
<ol class="lt-stages">
<li class="lt-stage">
<h3>Local</h3>
<p>One machine, no cloud account, no external services.</p>
<ul>
<li><a href="docs/lattice.storage.file/README.md">File write-ahead log</a></li>
<li><a href="docs/lattice.explorer/running-the-explorer.md">Explorer console</a> <span class="lt-status">in progress</span></li>
<li><a href="docs/lattice.api.mcp/README.md">MCP server</a></li>
</ul>
</li>
<li class="lt-stage">
<h3>Team</h3>
<p>A shared cluster with real users, so identity, policy, and data shape start to matter.</p>
<ul>
<li><a href="docs/lattice.membership/README.md">Membership</a>, with <a href="docs/lattice.membership.oidc/README.md">OIDC</a> or <a href="docs/lattice.membership.entra/README.md">Entra ID</a></li>
<li><a href="docs/lattice/security.md">Fail-closed authorization</a></li>
<li><a href="docs/lattice.schema/README.md">Schema</a></li>
<li><a href="docs/lattice.tenancy/README.md">Tenancy</a></li>
</ul>
</li>
<li class="lt-stage">
<h3>Global</h3>
<p>Multiple regions, each serving reads and writes.</p>
<ul>
<li><a href="docs/lattice.replication/README.md">Active-active replication</a></li>
<li><a href="docs/lattice.backup/README.md">Backup and disaster recovery</a></li>
<li><a href="docs/lattice.scaling/README.md">Autoscaling signal</a></li>
</ul>
</li>
</ol>
<p class="lt-invariant"><span class="lt-invariant-label">Programming model</span> <code>ILattice</code> <span class="lt-invariant-note">unchanged at every stage</span></p>
</section>
<section class="lt-seams" aria-labelledby="lt-seams-title">
<h2 id="lt-seams-title">A core plus seams</h2>
<p class="lt-section-lede">Storage, identity, governance, replication, administration, and observability are companion packages behind documented seams. A host takes only what it registers; a capability it leaves out costs nothing.</p>
<!-- lattice:seams -->
<p class="lt-section-more">Browse every package in the <a href="docs/index.md">documentation map</a>, or install from the <a href="PACKAGES.md">package inventory</a>.</p>
</section>
<section class="lt-close" aria-labelledby="lt-close-title">
<h2 id="lt-close-title">New here?</h2>
<p>The <a href="README.md">overview</a> explains what the platform is, why it exists, and how a deployment grows from one machine to many regions. Every capability is catalogued in <a href="FEATURES.md">Features</a>, each with its documentation and, where one exists, a runnable sample.</p>
</section>
</div>
