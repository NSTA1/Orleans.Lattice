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
for each, is `test/lattice.explorer.uitests/ConformanceChecklist.md`.

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
  appear in one toast region.
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

**Rendered conformance is checked in a browser lane.** An axe sweep runs the
`wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa` and `wcag22aa` rule sets over the
console in both themes, at every width band, signed in and signed out, and under
the high-contrast overlay. Explicit structural assertions cover what axe cannot
see, such as heading outlines, tab-to-panel relationships and live regions.

Two disciplines make those results mean something:

- **No suppression mechanism exists.** There is no allow-list to add an
  exception to. A finding is fixed, or it is tracked as its own issue.
- **Every case proves its own premises first.** Axe reports zero violations on a
  blank page, so each case first asserts that the console rendered, that the
  theme and contrast genuinely changed what the browser resolved, that the width
  band is the one requested, and that the identity is the one rendered. The rule
  set is checked for vacuity too: `target-size`, the only rule carrying the
  `wcag22aa` tag in the bundled axe-core, ships disabled, and
  `label-content-name-mismatch`, the only `wcag21a` rule, is tagged
  experimental, so both are force-enabled by id.

## Known limitations

- **The browser lane is being rebuilt for the rewritten console.** The rewrite
  replaced the whole UI, and the browser journeys written for the previous
  console were retired with it. Until the lane is re-baselined against the new
  areas, rendered coverage of the areas beyond Home is partial; the browserless
  and bUnit gates above are the dependable signal.
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
- **Coverage depends on what the test host can reach.** Areas that need a live
  cluster facade are reached only to the extent the test host serves one.
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

- `test/lattice.explorer.uitests/ConformanceChecklist.md` - the ten criteria and their enforcing tests
- [Theming and density](theming-and-density.md)
- [The Explorer navigation model](navigation-model.md)
- [Lattice Apps in the Explorer](lattice-apps.md)
