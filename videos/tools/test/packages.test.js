import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { workspaceRoot } from "../lib/hyperframes.js";
import { cameraCatalogue, OFF_CAMERA_SECTIONS, packageCatalogue, packagesScript, shortName } from "../lib/packages.js";

const markdown = [
  "# Orleans.Lattice Packages",
  "",
  "Intro.",
  "",
  "## Contents",
  "",
  "- [Core](#core)",
  "",
  "## Core",
  "",
  "The core package, plus the companions",
  "that extend the data model itself.",
  "",
  "| Package | NuGet | Description | Docs |",
  "|---|---|---|---|",
  "| `Orleans.Lattice` | [![NuGet](b)](n) | The core platform. | [Docs](docs/lattice/architecture.md) |",
  "| `Orleans.Lattice.Vector` | Unreleased | A vector index, with a | pipe. | [README](docs/lattice.vector/README.md) |",
  "",
  "## Explorer (in progress)",
  "",
  "The operator console.",
  "",
  "| Package | NuGet | Description | Docs |",
  "|---|---|---|---|",
  "| `Orleans.Lattice.Explorer` | [![NuGet](b)](n) | Console. | [README](docs/lattice.explorer/README.md) |",
  "",
  "## AI / RepoContext",
  "",
  "| Package | NuGet | Description | Docs |",
  "|---|---|---|---|",
  "| `Orleans.Lattice.RepoContext` | Unreleased | Context. | [README](docs/lattice.repocontext/README.md) |",
  "",
  "## Storage",
  "",
  "Durability backends.",
  "",
  "| Package | NuGet | Description | Docs |",
  "|---|---|---|---|",
  "| `Orleans.Lattice.Storage.File` | [![NuGet](b)](n) | Local disk. | [README](docs/lattice.storage.file/README.md) |",
  "",
  "## Related",
  "",
  "- [README](README.md)",
].join("\n");

test("names are shown as the site shows them", () => {
  assert.equal(shortName("Orleans.Lattice"), "Orleans.Lattice");
  assert.equal(shortName("Orleans.Lattice.Storage.File"), "Storage.File");
});

test("PACKAGES.md is read as the site reads it: sections with ledes, rows with release status", () => {
  assert.deepEqual(packageCatalogue(markdown), [
    {
      name: "Core",
      inProgress: false,
      lede: "The core package, plus the companions that extend the data model itself.",
      rows: [
        { id: "Orleans.Lattice", short: "Orleans.Lattice", released: true },
        { id: "Orleans.Lattice.Vector", short: "Vector", released: false },
      ],
    },
    {
      name: "Explorer",
      inProgress: true,
      lede: "The operator console.",
      rows: [{ id: "Orleans.Lattice.Explorer", short: "Explorer", released: true }],
    },
    {
      name: "AI / RepoContext",
      inProgress: false,
      lede: null,
      rows: [{ id: "Orleans.Lattice.RepoContext", short: "RepoContext", released: false }],
    },
    {
      name: "Storage",
      inProgress: false,
      lede: "Durability backends.",
      rows: [{ id: "Orleans.Lattice.Storage.File", short: "Storage.File", released: true }],
    },
  ]);
});

test("on camera: released packages only, no Explorer, and no section left empty", () => {
  assert.deepEqual(OFF_CAMERA_SECTIONS, ["Explorer"]);
  assert.deepEqual(cameraCatalogue(packageCatalogue(markdown)), [
    { name: "Core", lede: "The core package, plus the companions that extend the data model itself.", packages: ["Orleans.Lattice"] },
    { name: "Storage", lede: "Durability backends.", packages: ["Storage.File"] },
  ]);
});

test("the script defines the camera catalogue, and refuses a catalogue that does not open with the core", () => {
  const sandbox = {};
  new Function("window", packagesScript(markdown, "/* generated */\n"))(sandbox);
  assert.equal(sandbox.LatticePackages.sections[0].name, "Core");
  assert.ok(Object.isFrozen(sandbox.LatticePackages));
  assert.throws(() => packagesScript("## Storage\n\n| a | b | c | d |\n", ""), /must open with a Core section/);
});

test("the repository's own PACKAGES.md yields a camera catalogue that keeps the series rules", () => {
  const sections = cameraCatalogue(packageCatalogue(readFileSync(path.join(workspaceRoot, "..", "PACKAGES.md"), "utf8")));
  assert.equal(sections[0].name, "Core");
  assert.equal(sections[0].packages[0], "Orleans.Lattice");
  assert.ok(sections.length > 3, "the core and several seams");
  for (const section of sections) {
    assert.ok(section.packages.length > 0, section.name);
    assert.ok(!/Explorer/.test(section.name), "the Explorer is off camera");
    for (const name of section.packages) assert.ok(!/^Explorer/.test(name), `${section.name}: ${name}`);
  }
});
