# Explorer accessibility conformance checklist

This is the standard the Orleans.Lattice Explorer is held to, and the one the
browser lane in this directory enforces. It was written for epic #1845 ("a
coherent, accessible Explorer") by issue #1849, which widened the automated sweep
to WCAG 2.1 and 2.2 AA across both themes, all three breakpoint bands, both
identities, and every area the shell offers.

It lives here, next to the tests, rather than in `docs/`, because it is the
acceptance standard for the issues in that epic and each of them needs it before
the epic's documentation is written. The coordinator folds it into
`docs/lattice.explorer/` when the epic closes (#1859).

## How to use it

If you are implementing an issue in epic #1845, or any change to the Explorer's
UI:

1. Read the ten criteria below. They are what "accessible" means in this
   codebase; you should not have to rediscover it.
2. Run the browser lane before you claim the work is done (see
   [Running the lane](#running-the-lane)).
3. Never suppress a finding. There is no allow-list in this suite and no
   mechanism to add one, deliberately: see the note in
   `AccessibilityConformance.cs` for why the last one was removed along with the
   entry it held. A finding is either fixed or tracked as its own issue.
4. Never weaken an assertion to get a green run. Several assertions here are red
   against the code as it stands, on purpose, because this gate was landed before
   the fixes. The fixture doc comments name the issue expected to turn each one
   green.

The target is WCAG 2.2 level AA. Where a criterion below cites a success
criterion, that citation is the authority; the prose is a summary.

## The ten criteria

### 1. Keyboard operability and focus order

Every control is reachable and operable with a keyboard alone, and the order in
which focus moves matches the order the interface reads in. A composite widget
implements the keyboard behaviour its ARIA role implies: a `tablist` moves
between its tabs with the arrow keys, exposes exactly one tab to the tab
sequence at a time (a roving tabindex), and supports Home and End. Focus is
never trapped in a component the keyboard cannot leave.

WCAG SC 2.1.1 Keyboard (A), SC 2.1.2 No Keyboard Trap (A), SC 2.4.3 Focus Order (A).

Enforced by `AccessibilityStructureTests.Every_operable_tab_strip_moves_focus_with_arrow_keys` and
`.Every_tab_strip_exposes_a_roving_tabindex`.

### 2. Visible focus

Every control that can receive keyboard focus paints an indicator while it has
focus. `outline: none` is acceptable only when a replacement indicator is
painted. The indicator itself must meet the non-text contrast bar in criterion 8
and must not be obscured by other content.

WCAG SC 2.4.7 Focus Visible (AA), SC 2.4.11 Focus Not Obscured (AA, WCAG 2.2).

Enforced by `AccessibilityStructureTests.Every_keyboard_focus_stop_paints_a_visible_focus_indicator`.

### 3. Heading structure

Every surface has exactly one level-1 heading naming what the user is looking at,
and the heading outline below it never skips a level. Headings are the primary
means by which a screen-reader user navigates a page; a surface with no outline
can only be read linearly. Visual weight is not a heading: style a heading down
rather than marking up a non-heading as one.

WCAG SC 1.3.1 Info and Relationships (A), SC 2.4.6 Headings and Labels (AA).

Enforced by `AccessibilityStructureTests.Each_surface_has_one_h1_and_no_skipped_heading_levels`.

### 4. Landmarks and skip link

The shell exposes exactly one `main` landmark, at least one `navigation`
landmark, and a `banner`, on every surface and at every breakpoint band -
including when an area plugin has replaced the working surface, and including the
compact band where the catalog collapses behind a drawer. The first stop in the
tab order is a skip link that becomes visible when focused, targets the `main`
landmark, and actually moves focus there when activated.

WCAG SC 1.3.1 Info and Relationships (A), SC 2.4.1 Bypass Blocks (A).

Enforced by `AccessibilityStructureTests.The_shell_exposes_a_main_a_navigation_and_a_banner_landmark`
and `.A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main`. The same landmark check
re-runs after every area activation in
`AccessibilitySweepTests.Every_offered_area_has_no_critical_or_serious_wcag_violations`,
so it covers a plugin surface the moment one becomes reachable in the test host.

### 5. Live-region announcements

An asynchronous change the user did not directly cause to render - a surface
swap, a load completing, a save succeeding or failing, a scope change - is
announced in a live region. The region must already be in the accessibility tree
before the message arrives: a live region rendered at the same moment as its
content is silent. Use `role="status"` (polite) for progress and success, and
`role="alert"` (assertive) only for something the user must act on now.

WCAG SC 4.1.3 Status Messages (AA).

Enforced by `AccessibilityStructureTests.An_async_catalog_change_is_announced_in_a_polite_live_region`.

### 6. Name, role and value for custom widgets

Every custom widget exposes the name, role, state and relationships its role
implies, with values the ARIA specification actually permits. Two traps this
codebase has already fallen into, both of which an axe sweep passes:

- `aria-selected` is an *enumerated* attribute whose only valid tokens are
  `"true"` and `"false"`. Binding it to a C# `bool` renders a valueless HTML
  boolean attribute, which no tab may report. That was #1793.
- A `role="tab"` must name a real `role="tabpanel"` through `aria-controls`, and
  that panel must be labelled by a tab. A tab strip that announces the ARIA tabs
  pattern without implementing the relationship leaves a screen-reader user with
  nothing to move into.

WCAG SC 4.1.2 Name, Role, Value (A).

Enforced by `AccessibilitySweepTests.Every_tab_reports_a_valid_enumerated_aria_selected_value`
and `AccessibilityStructureTests.Every_tab_is_bound_to_a_real_tabpanel`.

### 7. Text contrast

Normal-size text meets 4.5:1 against every background it can sit on, and
large-scale text meets 3:1, in *both* palettes. "Every background it can sit on"
is the operative phrase: a token that clears the bar on the canvas can still fail
on a raised surface.

WCAG SC 1.4.3 Contrast (Minimum) (AA).

Guarded browserlessly, on every build, by
`Orleans.Lattice.Explorer.Tests.TextContrastTokenHygieneTests` - token arithmetic
does not need a browser, so it belongs in the required `build-and-test` check
rather than in this advisory lane. The axe sweep here re-measures the rendered
result in both themes as a second net.

### 8. Non-text contrast

User-interface components and the parts of a graphic needed to understand it meet
3:1 against their surroundings: control borders, focus indicators, the boundary
between a selected and an unselected state, and chart strokes. State must not be
carried by hue alone.

WCAG SC 1.4.11 Non-text Contrast (AA), SC 1.4.1 Use of Color (A).

This is the criterion the pre-#1849 gate could not see at all: the sweep ran
`wcag2a` / `wcag2aa` only, and 1.4.11 is a WCAG 2.1 criterion, so borders
measuring 1.21:1 passed by construction. The sweep now runs `wcag21aa`.

### 9. Reduced motion

A `prefers-reduced-motion: reduce` preference neutralises transitions,
animations, and smooth scrolling. Nothing moves for longer than a moment, and no
information is conveyed only by motion.

WCAG SC 2.3.3 Animation from Interactions (AAA, adopted here as a house rule).

Enforced by `AccessibilityStructureTests.A_reduced_motion_preference_neutralises_shell_motion`.

### 10. Forced colors and contrast preferences

The design system declares adaptations for `forced-colors` (Windows High Contrast
and its equivalents, which replace the author palette wholesale) and for
`prefers-contrast`. Under forced colors, state that was carried by a background
or a border colour must survive - which in practice means using system colour
keywords and not relying on a colour the user agent has just replaced.

No single WCAG success criterion covers this; it is how criteria 2, 8 and 1.4.1
survive contact with a user who has overridden the palette.

Enforced by `AccessibilityStructureTests.The_design_system_declares_contrast_preference_adaptations`.

## Running the lane

Browser tests are excluded from every default filter by category, so this does
not change an ordinary inner loop. Every fixture in this project carries
`[Category("UI")]` (enforced by `UiCategoryHygieneTests`) plus
`[Category("Integration")]` where it depends on the in-process web head.

```powershell
# Once per clone, and after a Microsoft.Playwright version bump
pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium

# The lane
dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj `
    --filter "TestCategory=UI" --nologo --blame-hang-timeout 5m --blame-hang-dump-type none
```

`.github/workflows/ui-tests.yml` is the lane's only CI runner and is path-filtered
to the Explorer UI. See the "Browser UI tier" section of
`.github/instructions/testing.instructions.md` for the tier rules, including when
a check belongs in bUnit instead.

## What the sweep covers, and what it cannot

The axe sweep runs the `wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa` and `wcag22aa`
rule sets over: both themes, all three breakpoint bands, signed in and signed
out (the full twelve-cell cross product on the home surface), and every area the
shell offers for each identity. Every case proves its own premises first - the
shell rendered, the theme genuinely changed what the browser resolved, the
viewport was genuinely classified into the band asked for, the identity is
genuinely the one rendered - because a sweep of a blank or misconfigured page
reports zero violations and would otherwise pass hardest when the app is most
broken.

It also proves the *rule set* is not vacuous. `target-size`, the only rule
carrying the `wcag22aa` tag in the bundled axe-core, ships disabled by default:
adding the tag without enabling the rule would have run zero WCAG 2.2 rules and
reported a clean WCAG 2.2 AA pass that meant nothing. Every requested tag is
checked against the tags of the rules axe actually evaluated.

Automated scanning still finds a minority of real barriers, and it is blind to
most of the criteria above - it cannot see whether a tab is bound to a panel,
whether a heading outline is navigable, whether a keyboard user can bypass the
chrome, or whether a change was announced. That is why criteria 1 to 6, 9 and 10
are asserted explicitly in `AccessibilityStructureTests` rather than left to the
sweep. `AxeMutationProof.md` in this directory records the mutation test proving
axe did not catch #1793, and is the honest evidence for that split.
