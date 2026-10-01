# Accessibility conformance

This is an honest statement of what the Orleans.Lattice Explorer targets, how
that is verified, and where the gaps are. A conformance statement that
overclaims is worse than none, so the limitations section is not boilerplate.

## What it targets

The Explorer targets **WCAG 2.2 Level AA**, which includes WCAG 2.1 Level AA.

Ten criteria are the working definition of "accessible" for this console:
keyboard operability and focus order; visible focus; heading structure;
landmarks and skip links; live-region announcements; name, role and value for
custom widgets; text contrast; non-text contrast; reduced motion; and forced
colours and contrast preferences. The full checklist, with the success criterion
and the enforcing tests for each, is [`ConformanceChecklist.md`](../../test/lattice.explorer.uitests/ConformanceChecklist.md).

## What the console does

- **Keyboard first.** Every control is reachable with the keyboard. The address
  line opens on `/` or `Ctrl+K` and is an ARIA 1.2 combobox; tabs follow the ARIA
  tabs pattern with arrow keys, Home and End; dialogs and sheets trap focus,
  close on Escape and return focus to the control that opened them.
- **Skip links and landmarks.** Every page starts with **Skip to directory**,
  **Skip to address** and **Skip to content**, and renders one `main` landmark
  with one level-1 heading.
- **The marker is never alone.** "You are here" and selected states are drawn
  with the marker colour and also with a ring, a heavier weight or markup such as
  `aria-current` and `aria-selected`, so no state is carried by colour alone.
- **Announcements.** The address line announces how many suggestions there are
  and which areas did not answer, in a polite status region, and notifications
  appear in one toast region. A tenant switch, which the address and header
  already show, is announced from that region without being drawn. A long-running tree operation's progress bar is an
  ARIA `progressbar` that describes itself in `aria-valuetext`; a change of step is
  announced once, and the moving percentage is not, so a reader is not interrupted
  on every poll.
- **Contrast.** Text clears 4.5:1 on every surface in both materials, and 7:1
  under the high-contrast overlay. Control boundaries, the focus ring, the
  selected ring and every state glyph clear 3:1, and 4.5:1 under the overlay.
  The overlay follows the platform's `prefers-contrast` unless you choose a
  contrast yourself (see [Theming and density](theming-and-density.md)).
- **Forced colours and motion.** The stylesheets declare `forced-colors`
  adaptations, and `prefers-reduced-motion: reduce` neutralises transitions and
  animations.
- **Touch and reflow.** Controls and rows are 44px high at comfortable density
  and never below 24px at compact density. Below 768px the console reflows to a
  single column: tables become two-line rows with a detail sheet, and no
  information depends on hover.

## How it is verified

**The design layer is checked without a browser, in the required build.**
Contrast is arithmetic over the design tokens, so it needs no rendering engine.
Browserless gates in the Explorer test project re-derive every text colour and
every non-text indicator from the shipped stylesheets, as a browser would resolve
them, and assert the floors above in both materials and under the high-contrast
overlay. Further hygiene gates fail the build when the marker paints anything
but a "you are here" or selected state, when a marked state is not also a ring,
a weight or markup, when an interactive primitive does not draw the focus ring,
and when a width is queried anywhere but the one breakpoint layer. Because these
run in the required `build-and-test` check, a regression fails the pull request.

**Components are checked in bUnit.** Every design primitive has component tests
for its roles, names, states and keyboard behaviour, including the compact table
form and the dialog focus trap, and the navigation chrome is tested the same way.

**Rendered conformance is checked in a browser lane.** A Playwright suite drives
the real web head against a live test cluster:

- **The axe sweep** runs the `wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa` and
  `wcag22aa` rule sets over every area's primary page, as the cluster's
  administrator, in all eight appearances (Paper and Board, standard and more
  contrast, comfortable and compact density), plus the signed-out Home and the
  sign-in dialog.
- **Named assertions** cover what axe cannot see. Keyboard tests walk the
  directory, open and restore the address line, drive the command palette, trap
  and return focus in the compact directory sheet, and move focus into and out of
  an app frame. Structure tests check one `h1` and no skipped heading level on every
  area page and a deeper one, the landmarks and skip links, the polite
  notification region, valid enumerated ARIA states, reduced motion, and that
  forced colours keep the current stop and the focus ring visible.
- **Reflow and target size.** Every area, on its primary page and a deeper one, is
  loaded at 360, 768 and 1280 pixels wide and must not scroll the page
  horizontally. On a phone every control must be at least 44 pixels in comfortable
  density and never below 24 in compact density.
- **Journeys** exercise first run, sign-in, re-authentication, a restricted
  identity, tenancy, deep links and address completion the way a person moves
  through them. The app frame's isolation is checked with hostile bundles in
  Chromium, Firefox and WebKit.

The criterion each test enforces is listed in the [`ConformanceChecklist.md`](../../test/lattice.explorer.uitests/ConformanceChecklist.md) checklist.

Three disciplines make those results mean something:

- **No suppression mechanism exists.** There is no allow-list to add an
  exception to. A finding is fixed, or it is tracked as its own issue.
- **Every case proves its own premises first.** Axe reports zero violations on a
  blank page, so each sweep first asserts that the page's heading rendered and that
  the document carries exactly the appearance asked for. The rule set is checked
  for vacuity too: every requested tag must resolve to at least one rule axe
  evaluated. `target-size`, the only rule carrying the `wcag22aa` tag in the
  bundled axe-core, ships disabled, and `label-content-name-mismatch`, the only
  `wcag21a` rule, is tagged experimental, so both are force-enabled by id.
- **The gates are mutation-tested.** A deliberate defect is applied to the source
  and the suite is run to show which test catches it.
  [`AxeMutationProof.md`](../../test/lattice.explorer.uitests/AxeMutationProof.md)
  records that the axe sweep passes an ARIA state bound to a C# `bool` (a valueless
  `aria-pressed`) and that a named assertion is what catches it.
  [`IsolationMutationProof.md`](../../test/lattice.explorer.uitests/Apps/IsolationMutationProof.md)
  records that the app frame stays contained when either of its two sandbox locks
  (the frame attribute or the bootstrap document's policy) is removed, and that
  removing both fails the isolation tests in all three engines.

## Known limitations

- **The browser lane is advisory, not a required check.** It is path-filtered
  to the Explorer, so unrelated pull requests do not provision a browser. Treat a
  failure as blocking by convention; nothing mechanically enforces that.
- **Only critical and serious findings fail the sweep.** A moderate or minor
  finding is reported but does not break the build. A clean run means "no
  critical or serious violation", not "no violation".
- **Automated scanning finds a minority of real barriers.** It cannot tell
  whether a heading outline is navigable or whether a change was announced.
  Those are asserted explicitly, but explicit assertions are still written by the
  same people who wrote the code.
- **Coverage depends on what the test cluster serves.** The cluster behind the
  axe sweep and the structure, keyboard and reflow tests runs no metrics backend
  and no tenancy add-on, so Telemetry and Tenancy are hidden from its
  administrator there. Their addresses render the not-found page, which is swept,
  reflowed and deep-linked like every other page. A second test cluster serves
  tenancy for the tenancy journeys: there an open tenant switcher, and the
  Tenancy directory with its **New tenant** dialog open, are swept in all eight
  appearances, but the rest of the Tenancy area's pages are not swept or
  reflow-tested, and no Telemetry page is swept in the browser.
- **Most of the lane runs in one engine.** Only the app frame's isolation, AppKit
  boot and task-board pilot tests run in Firefox and WebKit as well as Chromium; the accessibility
  sweep, structure, keyboard and reflow tests run in Chromium.
- **An app's own UI is out of scope.** A Lattice App's UI runs in a sandboxed
  frame and is the app author's responsibility. The kit stylesheet gives it the
  console's tokens, type and focus ring, but the console cannot verify what an
  app draws.
- **No formal third-party audit has been carried out**, and no testing with
  assistive-technology users has been done. Everything here is self-assessment.

## Reporting a problem

Accessibility defects are ordinary bugs and are tracked the same way. Open an
issue against the repository describing the barrier, the page, and the assistive
technology or interaction involved.

## See also

- [`ConformanceChecklist.md`](../../test/lattice.explorer.uitests/ConformanceChecklist.md) - the ten criteria and their enforcing tests
- [`AxeMutationProof.md`](../../test/lattice.explorer.uitests/AxeMutationProof.md) and [`IsolationMutationProof.md`](../../test/lattice.explorer.uitests/Apps/IsolationMutationProof.md) - the mutation evidence
- [Theming and density](theming-and-density.md)
- [The Explorer navigation model](navigation-model.md)
- [Lattice Apps in the Explorer](lattice-apps.md)
