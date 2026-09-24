---
name: Orleans.Lattice documentation
description: The order diagram. Ink on paper by day, chalk on slate by night, and one marker for the join.
colors:
  paper: "#ffffff"
  paper-sunken: "#f4f5f7"
  code-inline: "#edf0f4"
  ink: "#15191f"
  ink-secondary: "#4b5361"
  ink-tertiary: "#646c79"
  rule: "#dfe3e8"
  rule-strong: "#c3c9d1"
  link-blue: "#2234b0"
  link-blue-deep: "#17247d"
  marker: "#ffd23f"
  marker-soft: "#fff1bf"
  warning: "#8a5300"
  danger: "#b42318"
  success: "#1d7433"
  slate: "#101613"
  slate-sunken: "#0b100d"
  slate-raised: "#151c18"
  chalk: "#e4e9e5"
  chalk-secondary: "#a8b3ac"
  chalk-tertiary: "#8b978f"
  slate-rule: "#26302b"
  slate-rule-strong: "#3a4640"
  chalk-marker: "#f2d04b"
  chalk-blue: "#a9b8ff"
typography:
  display:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "clamp(2.3rem, 1.2rem + 3.1vw, 3.6rem)"
    fontWeight: 780
    lineHeight: 1.04
    letterSpacing: "-0.034em"
  h1:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "2.25rem"
    fontWeight: 780
    lineHeight: 1.2
    letterSpacing: "-0.024em"
  h2:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 720
    lineHeight: 1.2
    letterSpacing: "-0.016em"
  h3:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "1.1875rem"
    fontWeight: 650
    lineHeight: 1.2
    letterSpacing: "-0.01em"
  body:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.65
    letterSpacing: "normal"
  label:
    fontFamily: "Recursive Sans Linear, Segoe UI Variable Text, Segoe UI, system-ui, sans-serif"
    fontSize: "0.8125rem"
    fontWeight: 560
    lineHeight: 1.5
    letterSpacing: "normal"
  code:
    fontFamily: "Lattice Mono, Cascadia Mono, ui-monospace, SF Mono, Menlo, Consolas, monospace"
    fontSize: "0.8125rem"
    fontWeight: 400
    lineHeight: 1.6
    letterSpacing: "normal"
rounded:
  sm: "3px"
  md: "6px"
  pill: "999px"
spacing:
  "1": "0.25rem"
  "2": "0.5rem"
  "3": "0.75rem"
  "4": "1rem"
  "5": "1.5rem"
  "6": "2rem"
  "7": "3rem"
  "8": "4.5rem"
components:
  button:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.md}"
    padding: "0.45rem 0.95rem"
    height: "2.5rem"
  button-hover:
    backgroundColor: "{colors.marker-soft}"
  button-active:
    backgroundColor: "{colors.marker}"
    textColor: "{colors.ink}"
  button-quiet:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.md}"
  search-input:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    rounded: "{rounded.md}"
    height: "36px"
  code-block:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    rounded: "{rounded.md}"
    padding: "1rem 1.15rem"
  code-inline:
    backgroundColor: "{colors.code-inline}"
    textColor: "{colors.ink}"
    rounded: "{rounded.sm}"
  status-label:
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.pill}"
    padding: "0 0.45rem"
  join-figure-frame:
    backgroundColor: "{colors.paper}"
    rounded: "{rounded.md}"
    padding: "1.25rem 1.25rem 1rem"
---

# Design System: Orleans.Lattice documentation

The visual system of the documentation site built from `docs-site/`. The site
loads its values from `docs-site/template/public/tokens.css`. The frontmatter
above mirrors that file, and both change together. Other surfaces that must
match the site, such as the video series, read the same file.

## Overview

**The order diagram.** Orleans.Lattice is named after lattice-based state
primitives, whose merges are joins in a partial order. The site is drawn in that
notation: states are nodes, the order runs upward along hairline edges, and the
point where two concurrent writes meet, their join, is marked in one colour.
The notation is not decoration laid over the documentation. It is the
documentation's own wayfinding: the sidebar is a spine of page nodes, the
"In this article" rail is the page's chain of sections, the breadcrumb is a
short chain, and the current position in each is the marked node.

It comes in two materials, chosen by the reader's operating system or the theme
menu. **Paper** is ink on white, like a printed mathematics text read at a desk.
**Board** is chalk on a dark green-grey slate, like a seminar room's blackboard,
for readers who work in a dark editor. Both materials use the same structure,
weights, and rules. Only the pigments change.

The mode is Read. Long reference pages, dense tables, and compiled code samples
come first; the world lives in precise details (the node glyphs, the booktabs
rules, the marker) rather than in ornament around the text.

**The One Marker Rule.** Yellow is reserved for the join and for "you are here":
the current page, the current section, the current navigation tab, text
selection, and the join node of a figure. Nothing else is yellow, so the eye
learns that yellow means "this is where things meet".

**The Notation Rule.** When the site needs a new visual device, draw it in the
order-diagram vocabulary (a node, a spine, a chain, a join) before reaching for
a card, an icon, or a colour.

## Colors

The strategy is restrained: neutrals plus one accent that carries meaning. Every
text pair is checked to WCAG 2.2 AA or better in both materials.

### Primary

- **Ink** (`ink`, #15191f on paper; `chalk`, #e4e9e5 on slate): text, filled
  nodes, active edges, and the heavy rules of tables. 17.6:1 on paper, 14.9:1
  on slate.
- **Link blue** (`link-blue`): links and, in figures, the concurrent writes.
  It is the blue ink a typeset paper uses for its links, at 9.6:1 on paper. On
  slate, links take the chalk marker (`chalk-marker`, 12.1:1) and concurrent
  writes take chalk blue (`chalk-blue`, 9.6:1).

### Secondary

- **Marker** (`marker` on paper, `chalk-marker` on slate): the join and every
  "you are here" state. On paper the marker is pale (1.4:1 against white), so
  it always sits inside an ink ring or behind ink text and never carries a
  state alone. Ink on the marker is 12.2:1. The soft marker (`marker-soft`) is
  the highlighter band behind a hovered link, search matches, and a hovered
  button.

### Tertiary

- **Callout roles**: warning (`warning`, 6.1:1), danger (`danger`, 6.3:1),
  success (`success`, 5.8:1), and information in link blue. They colour only a
  callout's title, never its frame or its body.

### Neutral

- **Paper** (`paper`) is the page; **paper sunken** (`paper-sunken`) holds code,
  inputs, and quiet panels, and `code-inline` sits behind inline code.
- **Secondary and tertiary ink** (`ink-secondary` 7.8:1, `ink-tertiary` 5.3:1)
  carry descriptions, captions, and metadata.
- **Rules** (`rule`) separate table rows and sections; **strong rules**
  (`rule-strong`) draw spines, borders, hollow nodes, and a figure's resting
  edges.
- On slate, the same roles map to `slate`, `slate-sunken`, `slate-raised`,
  `chalk-secondary`, `chalk-tertiary`, `slate-rule`, and `slate-rule-strong`.

**The Marker Is Never Alone Rule.** A state marked in yellow is also marked by
weight, by an ink ring, or by text, so it survives greyscale, forced colours,
and the paler paper marker.

### Syntax

Code uses token roles rather than a stock theme: keywords in link blue, types in
teal (#0a6b62 on paper, #72d1c5 on slate), strings in green (#1d7433, #a8d98f),
numbers in rust (#a1401b, #f2b17a), comments in tertiary ink, and metadata and
attributes in plum (#7b3a99, #d7a9f2). They are the `--lt-syn-*` custom
properties in `tokens.css`, and every one clears 4.8:1 on its code background.
Comments are upright, not italic.

## Typography

**The One Family Rule.** One proportional family carries every voice, and
weight, not a second face, marks importance. Recursive, set as Recursive Sans
Linear, runs from the home page's display headline (780) through headings (720
and 650), labels (560), and body (400). Emphasis is a 10-degree oblique of the
same family, not a separate italic. Code, data, and the state labels of figures
take the one other face, Cascadia Mono.

Both are self-hosted, open-licensed (SIL OFL 1.1) variable fonts under
`docs-site/template/public/fonts/`, with their licences beside them, so pages
never fetch a font from a third party:

- `recursive-sans-linear.woff2`: Recursive with its MONO, CASL, and CRSV axes
  pinned to 0, keeping the weight (300 to 850) and slant (0 to -15 degrees)
  axes.
- `cascadia-mono.woff2`: Cascadia Mono, weight 300 to 700.

Both are subset to Latin, punctuation, arrows, and mathematical operators, and
the mono face keeps box drawing for the corpus's text diagrams. Anything
else, such as an emoji, falls back to the reader's system fonts.

### Hierarchy

- **Display** (clamp from 2.3rem to 3.6rem, 780, 1.04, -0.034em): the home
  page's thesis only.
- **h1** (2.25rem, 780), **h2** (1.5rem, 720), **h3** (1.1875rem, 650), and
  **h4** (1rem, 720): article headings, balanced, with more space above than
  below.
- **Body** (1rem, 400, line height 1.65): prose, held to a 42rem measure.
- **Label** (0.8125rem, 560): table headers, breadcrumbs, the rail, and
  metadata.
- **Code** (0.8125rem, Cascadia Mono, line height 1.6): blocks and inline code,
  with ligatures off.

## Layout

The frame is DocFX's `modern` template, restyled rather than replaced: a 60px
header, a 17.5rem sidebar, the article, and a 15rem "In this article" rail, all
within a 1480px container. From 768px to 1200px the sidebar narrows to 15rem.
The rail disappears below 1140px, and below 768px the sidebar becomes a
slide-in panel and the article takes the full width.

Prose, lists, headings, and callouts hold a 42rem measure. Tables, code, and
figures may run to the full width of the content column. A table wider than the
column scrolls inside its own frame rather than widening the page.

The home page is its own composition, authored at `docs-site/pages/index.md`:

1. A first viewport split roughly 54/46 between the thesis (headline, lede, and
   the three ways in) and the join figure. Below 1100px the two stack.
2. **The introduction**, the video series' front door: its player beside its
   facts (length, captions, the transcript on its companion page, and the
   download), under the episode's own title. It is generated from
   `docs/videos`, and the section is absent while no front-door episode is
   published.
3. **Three ways in**, Build, Evaluate, and Operate, each a chain of pages in
   reading order. These names are fixed, and the video series uses them too.
4. **One programming model, Local to Global**: the three deployment stages on a
   single chain, closed by the invariant `ILattice`.
5. **A core plus seams**: one node per section of `PACKAGES.md`, generated by
   `stage.ps1`, so a new package appears without an edit here.
6. A short close pointing to the overview and the capability catalogue.

**The Generated Map Rule.** Anything that lists packages, samples, or pages is
generated from `PACKAGES.md`, `FEATURES.md`, or the corpus by `stage.ps1`, never
maintained by hand, so the site cannot drift from the repository.

## Elevation & Depth

**The Hairline Rule.** Depth comes from 1px rules and from the sunken paper of
code and inputs, not from shadow. The page is flat. Only floating surfaces (the
theme menu and other dropdowns) take a shadow, a soft two-layer drop offset 1px
and 8px downward, because they sit above the page rather than on it.

## Shapes

- Corners are nearly square: 3px for inline code, keyboard keys, and small
  controls; 6px for buttons, inputs, code blocks, callouts, and figure frames.
  Status labels are pills.
- **Nodes are circles.** A page or section node is 7px, and the current page
  or section is a 9px marker node ringed at 1.5px. The home page's section and
  path nodes are 11px. A hollow node is the page colour inside a ring. Figures
  have their own geometry, described under the join figure.
- **Tables are booktabs.** A heavy rule above and below (1.5px ink), a lighter
  rule under the header (1px ink), and hairlines between rows. There are no
  vertical rules, zebra stripes, or cell boxes.
- Section headings (h2) open with a hairline and a small hollow node at its left
  end, so each section reads as a new node on the page's chain.

## Components

### Navigation

- **Header**: the mark and "Orleans.Lattice" (with "Orleans." in tertiary ink),
  the section tabs, the GitHub link, the theme menu, and search. The current tab
  is ink and heavier, with a 3px marker bar on the header's lower edge.
- **Search** focuses with "/", as on GitHub, and says so in a key hint.
- **Sidebar**: sections and packages head the tree in ink. Each package's pages
  hang from a hairline spine as hollow nodes, and the current page is the
  marker node. It opens scrolled so the current page is in view.
- **In this article**: the page's h2 and h3 sections as a chain. Passed sections
  fill in tertiary ink and the section in view is the marker node. The rail
  keeps an 8px inset so that node's ring is never clipped.
- **Breadcrumb**: short hairlines between crumbs. Section and package crumbs are
  labels, not links.
- **Previous and next**: at the article's foot, the destination title in ink
  above its direction label.

### Buttons

One outlined button: ink border, paper fill, 650 weight. Hover lays the soft
marker behind it and a press fills it with the marker. The quiet variant uses a
strong-rule border and secondary ink. Buttons appear only where the reader acts
on the page, such as a figure's controls, never as decoration.

### Status labels

"in progress" and "unreleased" are a small pill with a strong-rule border and
secondary ink. They appear wherever an unshipped package is named: in the
sidebar as a parenthesis after the package name, and as a pill on the
documentation map and the home page. On the map the
package's node is also hollow instead of filled, and a legend says so.

### Code and callouts

Code blocks sit on sunken paper inside a hairline frame. The copy button
appears on hover or focus, and always on touch screens.
Blockquotes are margin notes: set back on one strong hairline, in secondary ink.
Callouts (`> [!NOTE]` and its kin) take a hairline frame on sunken paper with
only the title in the callout's role colour.

### Documentation map

`docs/index.md` is generated: each section of `PACKAGES.md` with its lede, and
each package as a node carrying its name, its NuGet id and document count, and
the first sentence of its description. The samples index uses the same list.

### Video player

The browser's own player, never a custom one: `<video controls
preload="none" playsinline>` with a poster, one MP4 source, and an English
captions track. Nothing downloads until the reader presses play, and nothing
plays by itself. The frame is a hairline 16:9 box with 6px corners on sunken
paper, and the video's width and height attributes reserve the space before the
poster arrives, so the page never shifts. Focus is a 2px ring drawn inside the
frame. Under the player a caption row gives the length in mono, "English
captions", and a download link with the file size, separated by middle dots.
On the home page the facts sit in a booktabs list beside the player instead.

The episodes are rendered on paper white, so on slate a video reads as a lit
panel. That is a known limit of the media, not of the frame. `stage.ps1` emits
a player only when the episode's MP4, captions and poster are all present, and
the link gate checks the source and the track.

### Videos index

The Videos tab lists published episodes only, grouped by the video series' ways
in: Front door, Build, Evaluate, Operate, Deep dive: Secure, and Deep dive: How
it works. Each group is a chain of episodes in order, like the home page's ways
in. An episode is a node beside a 10rem poster thumbnail, with its title in
ink, its idea (the first sentence of its companion page) in secondary ink, and
its length in mono. The thumbnail repeats the title's link, so it is kept out
of the tab order. A group of one is a single node, drawn without a spine.

### The join figure (signature component)

The animated order diagram on the home page and on every CRDT explainer, where
it opens the page's Behaviour section. It shows one merge in the notation:
concurrent writes rise from a shared state, and each cluster's merge lifts it to
the join.

- **Source**: one entry per figure in `docs-site/figures/join-figures.json`,
  each mirroring the Behaviour example already on its page. `stage.ps1` renders
  the SVG and inserts it into the staged page only, so the tracked markdown is
  untouched. `main.js` animates it from the routes written onto its tokens.
- **Layouts**: a *diamond* (the bottom state, two incomparable writes, and the
  join) for most primitives; a *chain* with a middle node for registers over a
  total order, where the join is the higher write itself and the writer that
  reaches it directly travels a curve; and a two-node *chain* for a remove or
  disable that observed nothing, which stays at the bottom until the merge
  lifts it.
- **Geometry**: a 420-unit-high viewBox, 460 units wide on the home page and 560
  in an explainer. The bottom node sits at y 356, the writes at y 206 (138
  units either side of centre in a diamond), and the join at y 56. The bottom
  node is hollow (r 9), the writes are concurrent blue (r 9), and the join is
  the marker (r 13). Resting edges are 2 units in strong rule; drawn edges are
  2.25 units in ink.
- **Labels**: state in mono (15 units), each figure in its page's own notation
  (`A=3, B=5`, `P[A]=5`, `{alice, bob}`); notes and edge labels in sans (13
  units); the concurrency note in 12-unit oblique. On a phone an explainer's
  labels grow and the concurrency note shortens to "concurrent".
- **Motion**: plays once when the figure is half in view. Phase one (1300ms)
  draws the lower edges as the writes rise; phase two (1300ms) draws the upper
  edges as the merges lift both to the join, on cubic-bezier(0.45, 0, 0.2, 1).
  The join then pulses once (900ms). "Deliver again" sends one delta a second
  time (1400ms) to show that merging it changes nothing. A live caption narrates
  each phase.
- **Accessibility**: each SVG carries a title and a full prose description; the
  caption is a polite live region. With reduced motion, the figure shows the
  converged state and the buttons update the caption without moving anything.
  Without script the figure is complete, showing the converged state.
- **Tokens**: `--lt-diagram-node`, `-node-hollow`, `-node-ring`,
  `-concurrent`, `-join`, `-join-ring`, `-edge`, `-edge-active`,
  `-edge-width` (1.5px), `-node-size` (7px), `-node-size-lg` (11px), and
  `-ring-width` (1.5px) in `tokens.css`. The sidebar, the rail, the map, and the
  figures all draw from them.

### The mark

`docs-site/template/public/lattice-mark.svg` is the four-element lattice drawn
as a Hasse diagram: a hollow bottom node, two concurrent nodes, and the join as
a marker node at the top. In the header it is inlined so it follows the theme.
`favicon.svg` sets the same mark on an ink tile so it reads on light and dark
tab strips.

## Do's and Don'ts

### Do:

- **Do** keep yellow for the join and for "you are here", and pair it with a
  ring, weight, or text.
- **Do** draw new devices as nodes, spines, chains, and joins, in hairlines.
- **Do** generate anything that lists packages, samples, or pages from the
  repository's catalogues.
- **Do** label unreleased and in-progress work wherever it is named.
- **Do** give a new CRDT explainer its figure by adding an entry to
  `join-figures.json` that mirrors the page's own Behaviour example.
- **Do** keep every tracked asset plain ASCII; the repository's hygiene gates
  scan them.

### Don't:

- **Don't** use a second accent colour, a gradient, or a coloured bar wider than
  a hairline on a card, callout, or list item.
- **Don't** add cards, icon tiles, or feature grids. The site is read, not sold.
- **Don't** set a second proportional typeface, or use the mono face for
  anything that is not code, data, or a state.
- **Don't** add shadows to anything that sits on the page.
- **Don't** hand-write a figure's SVG or animate it outside `main.js`; change
  the JSON so every figure keeps one geometry and one motion.
- **Don't** fetch fonts, scripts, or images from a third party at view time.
- **Don't** autoplay a video, or load its bytes before the reader presses play.
