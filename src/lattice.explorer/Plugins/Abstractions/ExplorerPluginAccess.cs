namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The outcome of one plugin access probe: a state plus an optional
/// human-readable reason the shell may surface as a tooltip.
/// <para>
/// This is a <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> so a decision costs no allocation: the store keeps
/// one per plugin and the shell reads one per plugin per render. The
/// reason-free results are exposed as cached statics, so the common case is a
/// field read.
/// </para>
/// <para>
/// <c>default</c> is <see cref="ExplorerPluginAccessState.Denied"/> with no
/// reason, so an unprobed key fails closed.
/// </para>
/// </summary>
public readonly record struct ExplorerPluginAccess
{
    /// <summary>The caller may use the plugin.</summary>
    public static ExplorerPluginAccess Allowed { get; } = new(ExplorerPluginAccessState.Allowed, reason: null);

    /// <summary>The caller may not use the plugin. Identical to <c>default</c>.</summary>
    public static ExplorerPluginAccess Denied { get; } = new(ExplorerPluginAccessState.Denied, reason: null);

    /// <summary>The caller must sign in before the plugin can admit them.</summary>
    public static ExplorerPluginAccess AuthenticationRequired { get; }
        = new(ExplorerPluginAccessState.AuthenticationRequired, reason: null);

    /// <summary>The capability the plugin surfaces is not installed on this cluster.</summary>
    public static ExplorerPluginAccess Unavailable { get; }
        = new(ExplorerPluginAccessState.Unavailable, reason: null);

    private ExplorerPluginAccess(ExplorerPluginAccessState state, string? reason)
    {
        State = state;
        Reason = reason;
    }

    /// <summary>The resolved access state. <see cref="ExplorerPluginAccessState.Denied"/> by default.</summary>
    public ExplorerPluginAccessState State { get; }

    /// <summary>
    /// An optional human-readable explanation - for example the message of the
    /// fault that denied the probe - or <see langword="null"/> when none was
    /// supplied. Advisory display text only; never parsed.
    /// </summary>
    public string? Reason { get; }

    /// <summary>Whether the plugin is reachable and interactive.</summary>
    public bool IsAllowed => State == ExplorerPluginAccessState.Allowed;

    /// <summary>
    /// Whether the shell should render an entry for the plugin at all. Every
    /// state except <see cref="ExplorerPluginAccessState.Unavailable"/> is
    /// visible: a denial greys out rather than hides, so a caller can see that
    /// a surface exists and is not theirs.
    /// </summary>
    public bool IsVisible => State != ExplorerPluginAccessState.Unavailable;

    /// <summary>
    /// Returns an allowed result carrying <paramref name="reason"/>, or the
    /// cached <see cref="Allowed"/> when it is <see langword="null"/>.
    /// </summary>
    /// <param name="reason">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccess Allow(string? reason) =>
        reason is null ? Allowed : new ExplorerPluginAccess(ExplorerPluginAccessState.Allowed, reason);

    /// <summary>
    /// Returns a denied result carrying <paramref name="reason"/>, or the
    /// cached <see cref="Denied"/> when it is <see langword="null"/>.
    /// </summary>
    /// <param name="reason">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccess Deny(string? reason) =>
        reason is null ? Denied : new ExplorerPluginAccess(ExplorerPluginAccessState.Denied, reason);

    /// <summary>
    /// Returns an authentication-required result carrying
    /// <paramref name="reason"/>, or the cached
    /// <see cref="AuthenticationRequired"/> when it is <see langword="null"/>.
    /// </summary>
    /// <param name="reason">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccess RequireAuthentication(string? reason) =>
        reason is null
            ? AuthenticationRequired
            : new ExplorerPluginAccess(ExplorerPluginAccessState.AuthenticationRequired, reason);

    /// <summary>
    /// Returns an unavailable result carrying <paramref name="reason"/>, or the
    /// cached <see cref="Unavailable"/> when it is <see langword="null"/>.
    /// </summary>
    /// <param name="reason">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccess ReportUnavailable(string? reason) =>
        reason is null ? Unavailable : new ExplorerPluginAccess(ExplorerPluginAccessState.Unavailable, reason);
}
