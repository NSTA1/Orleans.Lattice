# Mutation-test record: what axe catches, and what it does not

This file records the mutation evidence for the accessibility gate in `Accessibility/`.
It says honestly what the axe-core sweep catches, what it misses, and which named
assertion is the real guard for what it misses. Both records were taken against the
rewritten Explorer (epic #3807) with this suite (issue #3832).

## 1. An ARIA state bound to a C# `bool` - axe does not catch it

### The mutation

`src/lattice.explorer/UI/Design/Components/LtButton.razor` renders a toggle's state as

```razor
aria-pressed="@AriaPressed"
```

where `AriaPressed` is the string `"true"` or `"false"` (or no attribute for a button that
is not a toggle). The mutation binds the `bool?` directly:

```razor
aria-pressed="@Pressed"
```

Blazor renders a `bool`-valued attribute as an HTML boolean attribute: present with no
value when `true`, absent when `false`. The ARIA specification defines `aria-pressed` as
an enumerated attribute (`true`, `false`, `mixed`), so every pressed toggle - the
appearance menu's choices, a segmented filter - then reports no valid state, and every
unpressed one reports that it is not a toggle at all. This is the same class of defect as
#1793, which bound `aria-selected` to a `bool` in the retired Explorer.

### The result

With the mutation applied, the full axe sweep
(`AccessibilitySweepTests`, nine areas in eight appearances, plus the signed-out home and
the sign-in dialog) **passed**: 10 of 10. axe's `aria-valid-attr-value` tolerates the
valueless form.

The named assertion **failed**:

```
Failed Every_control_reports_a_valid_enumerated_aria_state
  The Data area reports ARIA states outside their enumerated tokens.
  But was: < "aria-pressed="" on <button data-lt-command="appearance.theme.system" ...>System</but",
             "aria-pressed="" on <button data-lt-command="appearance.contrast.system" ...>System</",
             "aria-pressed="" on <button data-lt-command="appearance.density.comfortable" ...>Comf",
             "aria-pressed="" on <button type="button" class="lt-btn lt-btn--quiet" aria-pressed="">All</button>" >
```

`AccessibilityStructureTests.Every_control_reports_a_valid_enumerated_aria_state` is
therefore the guard for criterion 6 of `ConformanceChecklist.md`, and the sweep is not.

## 2. Link ink missing on Board - axe does catch it

This record is a real defect the sweep found, rather than a mutation made to prove it.

When this suite first ran, links in running content (a rule id in the Access table, a tree
in the Data directory, the address on the not-found page) had no colour of their own and
fell back to the browser's `#0000ee`, which measures 1.94:1 on Board's chalkboard:

```
Failed Every_area_primary_page_has_no_serious_violations_in_any_appearance("data")
  [serious] color-contrast: Elements must meet minimum color contrast ratio thresholds
    at a[href$="factory-floor"]: Element has insufficient color contrast of 1.94
    (foreground color: #0000ee, background color: #101613)
```

The same finding failed Access, Tenancy and Telemetry. The fix gives every such link the
documentation site's link ink on both materials, and its hover ink under the pointer
(`src/lattice.explorer/UI/wwwroot/design/lattice-primitives.css`, section 1). Both rules
are wrapped whole in `:where(...)`, so both have zero specificity and the later hover rule
wins. The first version wrote the base rule as `:where(.lt-viewport) a`, which has the
specificity of one element and silently beat the hover rule; with that form restored,
`AccessibilityStructureTests.A_link_in_running_content_takes_the_link_ink_and_the_hover_ink`
fails on both materials ("The hovered link does not take the hover ink."). Removing the
link rule altogether turns the four sweep cases red again.

## How to reproduce

1. Apply the mutation to `LtButton.razor`, delete the `:where(.lt-viewport a)` rule, or
   rewrite it as `:where(.lt-viewport) a`.
2. `dotnet build test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release`
3. `dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release --no-build --filter "FullyQualifiedName~Every_control_reports_a_valid_enumerated_aria_state|FullyQualifiedName~AccessibilitySweepTests|FullyQualifiedName~hover_ink"`
4. Restore the source and confirm `git diff src/` is clean.
