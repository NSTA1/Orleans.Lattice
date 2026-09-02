# Accessibility conformance

This is an honest statement of what the Orleans.Lattice Explorer targets, how
that is verified, and where the gaps are. A conformance statement that
overclaims is worse than none, so the limitations section is not boilerplate.

## What it targets

The Explorer targets **WCAG 2.1 Level AA and WCAG 2.2 Level AA**.

Ten criteria are treated as the working definition of "accessible" for this
console, and each is enforced by a named test rather than left to judgement:
keyboard operability and focus order; visible focus; heading structure;
landmarks and a skip link; live-region announcements; name, role and value for
custom widgets; text contrast; non-text contrast; reduced motion; and
forced-colors and contrast preferences. The full checklist, with the success
criterion and enforcing test for each, is
`test/lattice.explorer.uitests/ConformanceChecklist.md`.

## How it is verified

Verification is deliberately split across two lanes, because the two failure
modes need different tools.

**The token layer is checked without a browser, in the required build.** Colour
contrast is arithmetic over the design tokens, so it does not need a rendering
engine. A hygiene test parses the shipped stylesheet and asserts every text
pairing and every non-text pairing (borders, focus rings, state indicators)
against its floor, in both palettes, plus the ordering of the elevation levels.
Because it is browserless it runs in the required `build-and-test` check, so a
contrast regression fails the pull request rather than a nightly job.

**Rendered conformance is checked in a browser lane.** An axe sweep runs the
`wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa` and `wcag22aa` rule sets across both
themes, all three breakpoint bands, signed in and signed out, and every area the
shell offers, plus a targeted high-contrast pass over both palettes. Alongside
it, explicit structural assertions cover what axe cannot see, and a journey suite
exercises the console the way a person moves through it.

Two disciplines make those results mean something:

- **No suppression mechanism exists.** There is no allow-list to add an
  exception to. A finding is fixed, or it is tracked as its own issue. A
  `color-contrast` exemption once hid a real token defect, and removing the
  mechanism along with the entry is what stopped it being refilled.
- **Every case proves its own premises first.** Axe reports zero violations on a
  blank page, so a misconfigured sweep passes hardest exactly when the app is
  most broken. Each case asserts the shell rendered, the theme genuinely changed
  what the browser resolved, the viewport was genuinely classified into the band
  requested, the contrast overlay genuinely resolved different colour tokens
  from the standard one, and the identity is genuinely the one rendered, before
  asserting anything is clean. The rule set is checked for vacuity too:
  `target-size`, the only rule carrying the `wcag22aa` tag in the bundled
  axe-core, ships disabled,
  so requesting the tag without enabling the rule would have reported a
  meaningless clean WCAG 2.2 AA pass.

## Known limitations

- **Density is not driven by the sweep.** The axe sweep drives theme, breakpoint
  band, identity and contrast, but not `data-lx-density`, so the compact spacing
  scale has never been measured in a rendered DOM. It is a spacing concern rather
  than a contrast one, and `target-size` - the WCAG 2.2 AA rule this suite
  force-enables - is exactly the kind of rule a denser scale could regress.
- **Automated scanning finds a minority of real barriers.** It cannot tell
  whether a tab is bound to a panel, whether a heading outline is navigable,
  whether a keyboard user can bypass the chrome, or whether a change was
  announced. Those are asserted explicitly, but explicit assertions are still
  assertions written by the same people who wrote the code.
- **Only critical and serious findings fail the sweep.** The gate filters axe
  results to those two impact levels, so a moderate or minor finding is reported
  but does not break the build. A clean run therefore means "no critical or
  serious violation", not "no violation".
- **No formal third-party audit has been carried out**, and no testing with
  assistive-technology users has been done. Everything here is self-assessment.
- **The browser lane is advisory, not a required check.** It is path-filtered to
  the Explorer UI so unrelated pull requests do not provision a browser. Treat a
  failure as blocking by convention; nothing mechanically enforces that.
- **Coverage depends on what the test host can reach.** Areas that require a
  live cluster connection are not reachable in the harness, so they are swept
  only to the extent the harness can render them. The sweep logs which areas it
  reached and which it could not, on every run.
- **Third-party and host-rendered content is out of scope.** The statement
  covers the console's own surfaces.

## Reporting a problem

Accessibility defects are ordinary bugs and are tracked the same way. Open an
issue against the repository describing the barrier, the surface, and the
assistive technology or interaction involved.

## See also

- `test/lattice.explorer.uitests/ConformanceChecklist.md` - the ten criteria and their enforcing tests
- [Theming and density](theming-and-density.md)
- [The Explorer navigation model](navigation-model.md)
