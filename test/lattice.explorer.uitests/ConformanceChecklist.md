# Explorer accessibility conformance checklist

This is the standard the Orleans.Lattice Explorer is held to, and the one the browser lane
in this directory enforces. It was first written for epic #1845 and was carried over to the
rewritten Explorer (epic #3807) by issue #3832, which rebuilt the suite against the new
chrome: the directory spine, the address line and command palette, the two materials
(Paper and Board), the contrast and density choices, and the app frame. The published conformance statement is
`docs/lattice.explorer/accessibility-conformance.md`, which summarises this checklist and
points back to it as the enforcing reference.

## How to use it

1. Read the ten criteria below. They are what "accessible" means in this codebase.
2. Run the browser lane before you claim the work is done (see
   [Running the lane](#running-the-lane)).
3. Never suppress a finding. There is no allow-list in this suite and no mechanism to add
   one (see `Accessibility/AxeConformance.cs`). A finding is fixed or tracked as its own
   issue.
4. Never weaken an assertion to get a green run. A red assertion is a regression to fix.

The target is WCAG 2.2 level AA. Where a criterion cites a success criterion, that citation
is the authority; the prose is a summary.

## The ten criteria

### 1. Keyboard operability and focus order

Every control is reachable and operable with a keyboard alone, and focus moves in the order
the interface reads. The directory is walked stop by stop in directory order; the address
line opens on `/` or Ctrl+K, goes on Enter and restores on Escape; the command palette is
driven with the arrow keys through `aria-activedescendant` without focus leaving the input;
the compact directory sheet traps focus while open and returns it to its toggle when it
closes; and focus enters an app frame on Tab and leaves it on Shift+Tab, with "Leave app"
always reachable.

WCAG SC 2.1.1 Keyboard (A), SC 2.1.2 No Keyboard Trap (A), SC 2.4.3 Focus Order (A).

Enforced by `KeyboardAccessibilityTests.The_directory_is_reached_walked_and_followed_with_the_keyboard_alone`,
`.The_address_line_opens_goes_and_restores_with_the_keyboard_alone`,
`.The_command_palette_is_driven_with_the_keyboard_alone`,
`.The_directory_sheet_traps_focus_and_returns_it_when_it_closes`, and
`AppFrameKeyboardTests.Keyboard_focus_enters_and_leaves_an_app_frame`.

### 2. Visible focus

Every control that can receive keyboard focus paints an indicator while it has it, and the
indicator survives forced colours (an outline, which forced colours keep, rather than a
shadow, which they drop).

WCAG SC 2.4.7 Focus Visible (AA), SC 2.4.11 Focus Not Obscured (AA, WCAG 2.2).

Enforced by `KeyboardAccessibilityTests.Every_keyboard_focus_stop_paints_a_visible_focus_indicator`
and `AccessibilityStructureTests.Forced_colours_keep_the_current_stop_and_the_focus_ring_visible`.

### 3. Heading structure

Every page has exactly one visible level-1 heading naming what the user is looking at, and
the outline below it never skips a level - on every area's primary page and on a page deeper
in it, including the not-found page a hidden area's address renders.

WCAG SC 1.3.1 Info and Relationships (A), SC 2.4.6 Headings and Labels (AA).

Enforced by `AccessibilityStructureTests.Each_area_page_has_one_h1_and_no_skipped_heading_levels`.

### 4. Landmarks and skip links

The shell exposes one `main`, one `banner` and at least one `navigation` landmark at every
width. The first tab stop is the first of three skip links (directory, address, content),
which become visible when focused and move focus where they say.

WCAG SC 1.3.1 Info and Relationships (A), SC 2.4.1 Bypass Blocks (A).

Enforced by `AccessibilityStructureTests.The_shell_exposes_a_main_a_navigation_and_a_banner_landmark`
and `KeyboardAccessibilityTests.A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main`.

### 5. Live-region announcements

A change the user did not directly cause to render is announced in a live region that is
already in the accessibility tree before the message arrives. The Explorer's notification
region is a polite `status` region present, empty, on every page; the app frame host posts
every app notice into it, which the isolation suite relies on to read each hostile bundle's
report.

WCAG SC 4.1.3 Status Messages (AA).

Enforced by `AccessibilityStructureTests.The_notification_region_is_a_polite_live_region_before_anything_is_announced`.

### 6. Name, role and value for custom widgets

Every custom widget exposes the state its role implies with a value the ARIA specification
permits. A state bound to a C# `bool` renders a valueless attribute, which axe tolerates;
`AxeMutationProof.md` records that proof.

WCAG SC 4.1.2 Name, Role, Value (A).

Enforced by `AccessibilityStructureTests.Every_control_reports_a_valid_enumerated_aria_state`.

### 7. Text contrast

Normal text meets 4.5:1 and large text 3:1 against every surface it sits on, in both
materials and both contrast settings.

WCAG SC 1.4.3 Contrast (Minimum) (AA).

Enforced by `AccessibilitySweepTests.Every_area_primary_page_has_no_serious_violations_in_any_appearance`,
which sweeps every area in Paper and Board, standard and more contrast, comfortable and
compact density. The token arithmetic is also guarded without a browser in
`test/lattice.explorer`.

### 8. Non-text contrast

Control boundaries, focus indicators and state cues meet 3:1 against their surroundings, and
state is never carried by hue alone.

WCAG SC 1.4.11 Non-text Contrast (AA), SC 1.4.1 Use of Color (A).

Enforced by the same sweep, which runs the `wcag21aa` rules, and by
`AccessibilitySweepTests.The_signed_out_home_and_the_sign_in_dialog_have_no_serious_violations`
for the first thing every user sees.

### 9. Reduced motion

A `prefers-reduced-motion: reduce` preference neutralises every transition and animation.

WCAG SC 2.3.3 Animation from Interactions (AAA, adopted here as a house rule).

Enforced by `AccessibilityStructureTests.A_reduced_motion_preference_neutralises_shell_motion`.

### 10. Forced colours and contrast preferences

Under forced colours the platform's palette replaces the Explorer's, and every cue that was
carried by a colour survives: the current directory stop is still drawn differently from
every other stop, and focus is still an outline. The contrast preference is an appearance
the sweep covers in every area.

Enforced by `AccessibilityStructureTests.Forced_colours_keep_the_current_stop_and_the_focus_ring_visible`.

## Beyond the ten: reflow and target size

The responsive contract (epic #3807) makes the Explorer phone-first-class for reading and
simple actions. Every area, at 360, 768 and 1280 pixels wide, on its primary page and a
deeper one, reflows with no horizontal page scroll (WCAG 2.2 SC 1.4.10): content wider than
the viewport scrolls in its own frame. On a phone every control is at least 44 pixels in
comfortable density and never below 24 in compact density (SC 2.5.8, and the contract's
larger comfortable target).

Enforced by `ReflowTests.Every_area_reflows_without_horizontal_page_scroll` and
`ReflowTests.Every_control_on_a_phone_is_a_touch_target_for_its_density`.

## Running the lane

Browser tests are excluded from every default filter by category: every fixture here carries
`[Category("UI")]` (enforced by `UiCategoryHygieneTests`).

```powershell
# Once per clone, and after a Microsoft.Playwright version bump
pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium firefox webkit

# The lane
dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj `
    --filter "TestCategory=UI" --nologo --blame-hang-timeout 5m --blame-hang-dump-type none
```

`.github/workflows/ui-tests.yml` is the lane's own workflow, sharded from
`ui-test-shards.json`; the scheduled coverage workflow runs the same shards.

## What the sweep covers, and what it cannot

The axe sweep runs the `wcag2a`, `wcag2aa`, `wcag21a`, `wcag21aa` and `wcag22aa` rule sets
over every area's primary page as the test world's administrator, in all eight appearances,
plus the signed-out home and the sign-in dialog. Each case proves its premises first: the
page's heading rendered, the document carries exactly the appearance asked for, and every
requested tag resolved to at least one rule axe evaluated. `target-size` and
`label-content-name-mismatch`, the only rules behind `wcag22aa` and `wcag21a`, are withheld
from a tag-scoped run and are force-enabled by id.

The test world serves no metrics backend and no tenancy add-on, so Telemetry and Tenancy are
hidden from its administrator; what their addresses render - the not-found page - is swept,
reflowed and deep-linked like every other page.

Automated scanning finds a minority of real barriers and is blind to most of the criteria
above, which is why criteria 1 to 6, 9 and 10 are asserted by name rather than left to the
sweep.
