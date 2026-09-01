namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The colour themes the design system's token layer declares, and the
/// <c>data-theme</c> attribute value each is selected by.
/// <para>
/// The Explorer ships no theme switcher yet (that is issue #1852), so the light
/// palette is unreachable through the UI and was never swept - which is why the
/// light palette's own <c>--lx-color-text-dim</c> defect had to be found by hand
/// rather than by the gate. The token stylesheet already keys the palettes off
/// <c>:root[data-theme="dark"]</c> and <c>:root[data-theme="light"]</c>, so this
/// suite selects a theme exactly as the eventual switcher will: by setting the
/// attribute on the document element. <see cref="ExplorerShell.ApplyThemeAsync"/>
/// then proves the palette genuinely took effect by measuring a resolved token,
/// rather than trusting the attribute it just wrote.
/// </para>
/// </summary>
public enum ExplorerTheme
{
    /// <summary>The default palette, selected by <c>data-theme="dark"</c>.</summary>
    Dark,

    /// <summary>The light palette, selected by <c>data-theme="light"</c>.</summary>
    Light,
}
