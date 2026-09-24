// Shared stand-ins for the parts of the repository the tools read.

/** A docs-site home page (docs-site/pages/index.md) with the site's shape, small enough to read. */
export const homeFixture = `---
title: Documentation
---
<h1 id="lt-hero-title">State that lives in your cluster &amp; converges.</h1>
<a class="lt-way" href="#build"><span class="lt-way-name">Build</span><span class="lt-way-for">Writing code against <code>ILattice</code></span></a>
<a class="lt-way" href="#evaluate"><span class="lt-way-name">Evaluate</span><span class="lt-way-for">Deciding whether it fits</span></a>
<a class="lt-way" href="#operate"><span class="lt-way-name">Operate</span><span class="lt-way-for">Running a Lattice estate</span></a>
<h2 id="lt-paths-title">Three ways in</h2>
<h2 id="lt-journey-title">One programming model, Local to Global</h2>
<ol class="lt-stages">
<li class="lt-stage">
<h3>Local</h3>
<p>One machine, no cloud account.</p>
<ul>
<li><a href="a.md">File write-ahead log</a></li>
<li><a href="b.md">Explorer console</a> <span class="lt-status">in progress</span></li>
</ul>
</li>
<li class="lt-stage">
<h3>Team</h3>
<p>A shared cluster.</p>
<ul>
<li><a href="c.md">Membership</a>, with <a href="d.md">OIDC</a> or <a href="e.md">Entra ID</a></li>
</ul>
</li>
</ol>
<p class="lt-invariant"><span class="lt-invariant-label">Programming model</span> <code>ILattice</code> <span class="lt-invariant-note">unchanged at every stage</span></p>
<h2 id="lt-seams-title">A core plus seams</h2>
<p class="lt-section-lede">Storage and identity are companion packages.</p>
`;

/** A PACKAGES.md with one released core package. */
export const packagesFixture = [
  "# Packages",
  "",
  "## Core",
  "",
  "The core package.",
  "",
  "| Package | NuGet | Description | Docs |",
  "|---|---|---|---|",
  "| `Orleans.Lattice` | [badge](x) | The core. | [Docs](docs/lattice/a.md) |",
  "",
].join("\n");
