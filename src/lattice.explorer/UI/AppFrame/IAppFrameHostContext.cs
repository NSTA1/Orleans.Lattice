namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// What the host tells a frame about its surroundings: the appearance, the active tenant's
/// display name, and the signed-in user's display name. Registered scoped; the chrome (S1),
/// session (S2) and tenancy (A5) items may replace the default registration with one that
/// reads their own state.
/// </summary>
/// <remarks>
/// A frame only ever sees display names, never an id. The user's display name reaches a
/// frame only through <c>context.user</c>, and only when the install's consented bridge set
/// grants it.
/// </remarks>
internal interface IAppFrameHostContext
{
    /// <summary>The current appearance.</summary>
    AppFrameAppearance Appearance { get; }

    /// <summary>The active tenant's display name, or <see langword="null"/> when tenancy is off or unknown.</summary>
    string? TenantDisplayName { get; }

    /// <summary>The signed-in user's display name, or <see langword="null"/> when unknown.</summary>
    string? UserDisplayName { get; }

    /// <summary>
    /// Records the appearance the Explorer's own page is drawn in, as the frame host read it
    /// from the document, so <see cref="Appearance"/> reports it from then on. The default
    /// ignores it; an implementation that keeps it sanitises it first.
    /// </summary>
    /// <param name="appearance">The appearance read from the page.</param>
    void ObserveAppearance(AppFrameAppearance appearance)
    {
    }
}
