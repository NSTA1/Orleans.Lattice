---
agent_spec: "docs/agents/capabilities.yaml"
---

# Theming and density

The Explorer draws in the documentation site's visual world and adds an operator-console layer for density, focus, state roles, and app frames. Appearance is made of three choices: theme, contrast, and density.

## Theme

The theme choice is the material:

- System follows `prefers-color-scheme`.
- Paper stores and applies `light`.
- Board stores and applies `dark`.

The active material is applied to the document element as `data-bs-theme="light"` or `data-bs-theme="dark"`. The system choice resolves to one of those values for rendering, and the chrome listens for operating-system colour-scheme changes while System is selected.

## Contrast

The contrast axis has three choices:

- System leaves `data-lt-contrast` unset and lets CSS follow `prefers-contrast: more`.
- Standard writes `data-lt-contrast="standard"` and opts out of the system high-contrast overlay.
- More writes `data-lt-contrast="more"` and applies the high-contrast overlay.

The high-contrast overlay is not a label-only duplicate. Its tokens target 7:1 text contrast and 4.5:1 non-text contrast on Paper and Board.

## Density

Density changes row and control heights without changing the address or the data being read.

- Comfortable is the default. Rows and controls are 44px, so ordinary actions are touch targets.
- Compact sets `data-lt-density="compact"`. Rows and controls are 28px, still above the 24px WCAG 2.2 minimum target size.

When comfortable is selected the density attribute is removed, so the default CSS tokens apply.

## Fields and toolbars

Every field - a text box, a name box, a picker, a multi-value picker, a select, a search box, a date and time field or a duration field - is a visible label row over a control box. The label row is one line, and the control box is one control height (44px comfortable, 28px compact) with the same border, fill and padding in Paper and Board, so fields of every kind line up beside each other. A search box's label is visible like any other and is its accessible name.

A toolbar lines its controls up on their control boxes. In a toolbar that holds a labelled field, a button, a segmented choice, a switch, a checkbox or a count starts one label row down, on the control row; a field's hint or error grows the field downwards without moving a control. Below 768px a toolbar stacks one item per line. A placeholder is always prose in the interface face, even in a field whose value is an id in the monospace face.

DESIGN.md (Fields and toolbars) holds the rule and names the tests that enforce it.

## Dates, times and durations

A field that takes a point in time is never a bare text box. The date and time field is one control box holding the instant, typed or shown as ISO 8601 to the second (`2026-09-28T14:05:00Z`), the zone it is in, and a calendar button. The zone is always UTC and always written in the box. A typed time that names another offset is read as the instant it names and rewritten in UTC in the box, so nothing is converted silently. Once the page is interactive, the reader's own local time is shown under the box as secondary text. A field may allow an empty value, whose meaning (such as **Latest**) is its placeholder, and it may refuse times before a minimum, after a maximum, or in the future, with the reason shown under the box.

The calendar button opens a picker below the field, or in the flow of the page on a phone. It has quick picks (Now, 1 hour ago, 24 hours ago, 7 days ago; one outside the field's range is disabled), a month grid starting on Monday, a time to the second in UTC, and **Done** (with **Clear** when the field may be empty). In the grid the arrow keys move by a day or a week, Page Up and Page Down by a month (with Shift, by a year), and Home and End to the start and end of the week. Escape closes the picker and returns focus to its button.

A field that takes a duration has a whole-number box for each unit it offers - days, hours, minutes or seconds - in one control box, each named by the field and its unit. A box that is not a whole number, or a total outside the field's range, is refused with the reason shown under the box. `ShellTimeFieldHygieneTests` fails the build when a field whose label, hint or placeholder names a date, a time, UTC, ISO or a duration is drawn as a plain text box.

## Sizes

Byte sizes are written in binary units (`B`, `KiB`, `MiB`, `GiB` and larger units where a surface offers them). The unit is chosen from the figure as written: a value just under the next unit boundary moves up instead of reading as `1024` of the smaller unit.

## First paint and document attributes

A classic blocking script in the document head reads the small appearance record `orleans.lattice.explorer.appearance.v2` from local storage. It accepts only shipped names, resolves System against the operating system, and sets these attributes before the first paint:

- `data-bs-theme`
- `data-lt-contrast`
- `data-lt-density`

This avoids a flash of the wrong material. The script is only a paint helper; after startup, the app restores the declared preference keys and applies appearance through the chrome module.

## Appearance menu and commands

The header appearance menu renders one toggle button per choice. Each button carries the same `data-lt-command` id as the command palette command that performs the action:

- `appearance.theme.system`
- `appearance.theme.paper`
- `appearance.theme.board`
- `appearance.contrast.system`
- `appearance.contrast.standard`
- `appearance.contrast.more`
- `appearance.density.comfortable`
- `appearance.density.compact`

The menu state uses pressed buttons, so the visible control and the palette are two entries to the same action, not separate behaviours.

## Reduced motion

The design layer respects `prefers-reduced-motion: reduce` by reducing transition and animation durations to `0.01ms` and limiting animation iteration to one. This is a global rule for the Explorer UI.

Lattice App frames receive appearance through the app-frame protocol. The bundle and `context.read` include `{ theme, contrast, density, reducedMotion }`, and the protocol can notify a running frame with `context.changed` carrying the same closed set. When a frame handshake completes, the host reads the appearance already applied to the Explorer page and includes that snapshot in the bundle and `context.read`; if the read fails, the host keeps its safe fallback of Paper, standard contrast, comfortable density and full motion. The current Explorer does not notify an already-running frame when the page appearance changes. Reopen the app to receive the current appearance.

## See also

- [Explorer overview](README.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Navigation model](navigation-model.md)
- [Areas](areas.md)
- [Lattice Apps](lattice-apps.md)
- [Accessibility conformance](accessibility-conformance.md)
