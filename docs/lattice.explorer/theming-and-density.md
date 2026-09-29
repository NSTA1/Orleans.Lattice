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

Lattice App frames receive appearance through the app-frame protocol. The bundle and `context.read` include `{ theme, contrast, density, reducedMotion }`; a running frame can be notified with `context.changed` carrying the same closed set. The frame vocabulary is Paper or Board, standard or more contrast, comfortable or compact density, and a reduced-motion boolean. The fallback host context is Paper, standard contrast, comfortable density, and full motion unless a host supplies a richer context.

## See also

- [Explorer overview](README.md)
- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Navigation model](navigation-model.md)
- [Areas](areas.md)
- [Lattice Apps](lattice-apps.md)
- [Accessibility conformance](accessibility-conformance.md)
