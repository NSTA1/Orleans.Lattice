namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// One circuit's header panels - the appearance menu, the tenant switcher, the
/// identity menu and the session modals - kept to one open at a time: each
/// announces itself as it opens, and every other one closes on the announcement.
/// </summary>
/// <remarks>
/// It is registered scoped, so a circuit's panels only ever close each other. An
/// announcement is raised synchronously on the caller's thread; a component
/// subscriber changes its own state and re-renders through <c>InvokeAsync</c>.
/// </remarks>
internal sealed class ShellHeaderPanels
{
    /// <summary>Raised as a panel opens, with the panel that is opening.</summary>
    public event Action<object>? Opened;

    /// <summary>Announces that <paramref name="panel"/> is opening, so every other panel closes.</summary>
    /// <param name="panel">The opening panel, which ignores its own announcement.</param>
    /// <exception cref="ArgumentNullException"><paramref name="panel"/> is <see langword="null"/>.</exception>
    public void Opening(object panel)
    {
        ArgumentNullException.ThrowIfNull(panel);
        Opened?.Invoke(panel);
    }
}
