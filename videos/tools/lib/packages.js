// The package catalogue, read from PACKAGES.md at the repository root exactly
// as the documentation site reads it (docs-site/stage.ps1,
// Get-PackageCatalogue): one entry per "## " section except Contents and
// Related, each with its lede and its table rows. DESIGN.md's Generated Map
// Rule applies to the videos too: anything that lists packages is generated
// from this file, never typed into a composition.

/** Sections the series keeps off camera: the Explorer, while it is redesigned (series.md). */
export const OFF_CAMERA_SECTIONS = Object.freeze(["Explorer"]);

/** A package's name as the site shows it: without the Orleans.Lattice. prefix, except the core itself. */
export function shortName(id) {
  return id === "Orleans.Lattice" ? id : id.replace(/^Orleans\.Lattice\./, "");
}

/** Parses PACKAGES.md into [{ name, inProgress, lede, rows: [{ id, short, released }] }]. */
export function packageCatalogue(markdown) {
  const sections = [];
  let current = null;
  let lede = null;
  for (const line of markdown.replace(/\r\n/g, "\n").split("\n")) {
    const heading = /^##\s+(.+?)\s*$/.exec(line);
    if (heading) {
      current = null;
      if (heading[1] === "Contents" || heading[1] === "Related") continue;
      current = {
        name: heading[1].replace(/\s*\(in progress\)\s*$/, ""),
        inProgress: /\(in progress\)\s*$/.test(heading[1]),
        lede: null,
        rows: [],
      };
      sections.push(current);
      lede = [];
      continue;
    }
    if (!current) continue;
    if (line.startsWith("|")) {
      if (lede?.length) current.lede = lede.join(" ");
      lede = null;
      const cells = line.split("|");
      const id = /^\s*`([^`]+)`\s*$/.exec(cells[1] ?? "");
      if (cells.length < 6 || !id) continue;
      current.rows.push({ id: id[1], short: shortName(id[1]), released: !/Unreleased/.test(cells[2]) });
      continue;
    }
    if (lede !== null) {
      if (line.trim() === "") {
        if (lede.length) {
          current.lede = lede.join(" ");
          lede = null;
        }
      } else {
        lede.push(line.trim());
      }
    }
  }
  return sections;
}

/**
 * The catalogue as the series may show it: released packages only, without
 * the off-camera sections, and without any section that is then empty.
 */
export function cameraCatalogue(sections, offCamera = OFF_CAMERA_SECTIONS) {
  return sections
    .filter((section) => !offCamera.includes(section.name))
    .map((section) => ({
      name: section.name,
      lede: section.lede,
      packages: section.rows.filter((row) => row.released).map((row) => row.short),
    }))
    .filter((section) => section.packages.length > 0);
}

/** The camera catalogue as a script that compositions load before any component mounts. */
export function packagesScript(markdown, generatedBanner) {
  const sections = cameraCatalogue(packageCatalogue(markdown));
  if (sections.length === 0 || sections[0].name !== "Core") {
    throw new Error("packages: PACKAGES.md must open with a Core section that has a released package");
  }
  return `${generatedBanner}window.LatticePackages = Object.freeze(${JSON.stringify({ sections }, null, 2)});\n`;
}
