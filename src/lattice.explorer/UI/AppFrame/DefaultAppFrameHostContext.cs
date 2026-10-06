namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The fallback <see cref="IAppFrameHostContext"/>: the appearance the Explorer's page is
/// drawn in, once the frame host has read it (the default appearance until then), and no
/// tenant or user display name, so <c>context.user</c> answers <c>unavailable</c> until the
/// session chrome supplies a real one.
/// </summary>
/// <remarks>
/// Registered scoped, one per circuit, so an appearance observed in one browser window is
/// never reported to a frame in another.
/// </remarks>
internal sealed class DefaultAppFrameHostContext : IAppFrameHostContext
{
    /// <inheritdoc />
    public AppFrameAppearance Appearance { get; private set; } = AppFrameAppearance.Default;

    /// <inheritdoc />
    public string? TenantDisplayName => null;

    /// <inheritdoc />
    public string? UserDisplayName => null;

    /// <inheritdoc />
    public void ObserveAppearance(AppFrameAppearance appearance)
    {
        ArgumentNullException.ThrowIfNull(appearance);
        Appearance = appearance.Sanitise();
    }
}
