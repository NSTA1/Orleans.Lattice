# Mutation-test record: #1793 (aria-selected bound to a bool)

This file records the mutation-test evidence for the accessibility baseline in
`AccessibilitySweepTests.cs`. It documents, honestly, exactly what axe-core does and
does not catch for the #1793 regression, and which assertion is the real guard.

## The regression

#1793: in `src/lattice.explorer/UI/Navigation/AppShell.razor` the home area tab bound
its selection state with a bare C# `bool`:

```razor
aria-selected="@(_activePlugin is null)"
```

Blazor renders a `bool`-valued attribute as an HTML boolean attribute: when the value is
`true` the attribute is emitted with no value at all (`aria-selected`), and when `false`
it is omitted entirely. The ARIA spec defines `aria-selected` as an *enumerated* attribute
whose only valid tokens are `"true"` and `"false"`, so the active tab reported no valid
enumerated value. The fix binds an explicit string:

```razor
aria-selected="@(_activePlugin is null ? "true" : "false")"
```

## What the rendered DOM looks like under the mutation

With the buggy `bool` binding, the active home tab renders (captured from a live web head):

```html
<button type="button" role="tab" class="lx-shell-area-tab is-active" aria-selected>Explore</button>
```

That is, `aria-selected` with **no value**. Read back through the DOM API
(`getAttribute('aria-selected')`) this is the empty string `""`.

## Finding: axe-core does NOT catch this

Running the axe-core `wcag2a` / `wcag2aa` rule set over the mutated home surface reported
**zero** critical or serious violations - `Home_surface_has_no_critical_or_serious_wcag_violations`
still passed. axe's `aria-required-attr` / `aria-valid-attr-value` handling is satisfied by
the mere presence of `aria-selected` and tolerates the valueless boolean-attribute form, so
the automated sweep alone would not have caught #1793.

This is reported honestly rather than worked around: the brief anticipated this outcome and
directed that if axe does not flag the mutation, we say so and assert the attribute directly.

## The real guard: a direct enumerated-value assertion

`Every_tab_reports_a_valid_enumerated_aria_selected_value` asserts that every `role="tab"`
element carries an `aria-selected` attribute whose value is exactly `"true"` or `"false"`.
This is the assertion that catches #1793.

### Mutation-test output (buggy source)

Recorded when the tab was still bound in `AppShell.razor`. The binding has since
moved into the design system's single tab primitive, and the test now runs once
per breakpoint band and reports a per-band count, so a present-day run prints a
different message. With the `bool` binding restored to `AppShell.razor` line 51,
the test failed:

```
role=tab element at index 0 has aria-selected="", which is not a valid enumerated value.
A valueless (boolean-attribute) aria-selected renders as null here and is the exact #1793 regression.
  Assert.That(value, Is.EqualTo("true").Or.EqualTo("false"))
  But was:  <string.Empty>

Failed!  - Failed: 1, Passed: 0, Skipped: 0, Total: 1
```

### On fixed source

With the correct string binding, both the axe sweep and the enumerated-value assertion pass.

## How to reproduce

Every tab strip in the shell - the area rail included - now renders through
`LatticeAdaptiveTabs`, so the enumerated binding lives there once.

1. In `src/lattice.explorer/DesignSystem/Components/LatticeAdaptiveTabs.razor`, change the
   binding `aria-selected="@(isActive ? "true" : "false")"` back to the buggy `bool` form
   `aria-selected="@isActive"`.
2. `dotnet build test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release`
3. `dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release --no-build --filter "FullyQualifiedName~Every_tab_reports_a_valid_enumerated_aria_selected_value"`
4. Observe a failure at every breakpoint band, reporting the tabs whose `aria-selected` reads as
   an empty string, then restore the binding and confirm `git diff src/` is clean.
