# Theming and density

The console ships two palettes and three densities, and both are the user's
choice. Dark remains the default when no preference is expressed.

## Theme

The theme control offers:

| Choice | Behaviour |
| --- | --- |
| Follow system | Honours the browser or platform `prefers-color-scheme`. This is the default. |
| Light | Always light. |
| Dark | Always dark. |

Light is a first-class palette, not a broken opt-in. It carries four distinct,
ordered surface levels, so elevation survives: a raised menu, a plain panel and
a sunken well are visually different rather than three shades of white
separated by an invisible border.

## Contrast is a separate axis

High contrast is **not** a third theme. Contrast layers over whichever theme is
active:

| Attribute | Values | Where it is set |
| --- | --- | --- |
| `data-theme` | `light`, `dark` | `<html>` |
| `data-contrast` | `more`, `standard` | `<html>` |
| `data-lx-density` | `comfortable`, `cosy`, `compact` | `<body>` |

Keeping contrast orthogonal avoids a combinatorial set of palettes that would
have to be kept in step with each other. `data-contrast="more"` raises contrast
over the active theme; `data-contrast="standard"` opts back out of the
platform's own contrast hint.

The token layer also honours `prefers-contrast: more` and `forced-colors`, so
Windows High Contrast is respected without the user configuring anything.

Density is set on `<body>` rather than `<html>` deliberately: the token layer
declares each density preset at attribute specificity, and the breakpoint layer
must still be able to win.

## Density

| Choice | Behaviour |
| --- | --- |
| Automatic | Density follows the breakpoint, as before. This is the default. |
| Comfortable / Cosy / Compact | An explicit choice, which overrides the breakpoint-derived value. |

Choosing a density pins it. Leaving it unset preserves the adaptive behaviour,
so nothing changes for a user who never opens the setting.

## Applying a theme without a flash

A light-theme user who sees the dark palette for a moment on every load does not,
in practice, have a light theme. The chosen appearance is therefore on the
document at **first paint**, not applied after the application starts.

Two things make that impossible to solve in the obvious places:

- **The server cannot answer it.** The preference lives in browser storage, and
  the head is rendered before any circuit exists, so the markup the server
  produces cannot know the answer.
- **A component cannot answer it.** The attributes belong on `<html>` and
  `<body>`, above every component's render tree, and a component only runs after
  hydration, which is already too late.

What does work is a classic blocking script in `<head>`: the parser stops on it,
and it has not yet reached `<body>`, so nothing has been laid out or painted. It
must stay a classic script, it must stay in `<head>`, and it must not gain
`defer` or `async`. Any of those changes reintroduces the flash on every load.

That script reads a small, plain record of the last applied appearance. The
durable preference contract remains the only thing that *remembers* a
preference, and on the web head it is encrypted with Data Protection so script
cannot read it. The first-paint record is therefore a cache, refreshed whenever
an appearance is applied. When the two disagree the contract wins, because the
application rewrites the record from it as soon as it loads. The values are
non-secret presentation names validated against a fixed allow-list on the way
in, so a tampered record can only ever select an appearance the product already
ships.

## Both heads honour the choice

The web head persists through browser storage; the desktop head persists through
the platform preference store and behaves sensibly against its host platform's
own theme. The choice is scoped per user.

## Where the control lives

Appearance settings sit in the console banner, in their own labelled region
between the tenant scope control and the identity. They are deliberately not
tucked inside the sign-out cluster, which is where the old tenant control lived
and where nobody found it.

The console renders the control only when the appearance feature is registered.
It is an opt-in service, so a head that composes without it gets a console with
no appearance settings rather than a console that fails to start.

## See also

- [What the Explorer remembers](what-the-explorer-remembers.md)
- [Accessibility conformance](accessibility-conformance.md)
- [Running the Explorer](running-the-explorer.md)
