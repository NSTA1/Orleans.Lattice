namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// The fallback <see cref="IAppFrameHostContext"/>: the default appearance and no tenant or
/// user display name, so <c>context.user</c> answers <c>unavailable</c> until the session
/// chrome supplies a real one.
/// </summary>
internal sealed class DefaultAppFrameHostContext : IAppFrameHostContext
{
    /// <inheritdoc />
    public AppFrameAppearance Appearance => AppFrameAppearance.Default;

    /// <inheritdoc />
    public string? TenantDisplayName => null;

    /// <inheritdoc />
    public string? UserDisplayName => null;
}
