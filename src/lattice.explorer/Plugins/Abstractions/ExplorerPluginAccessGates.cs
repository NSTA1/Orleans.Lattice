namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Ready-made <see cref="IExplorerPluginAccessGate"/> implementations for the
/// cases that need no probe at all, plus a delegate adapter for a plugin that
/// wants an inline rule rather than its own gate type.
/// <para>
/// The fixed gates are cached singletons, so a plugin that is always reachable
/// (or always absent) costs no allocation and no probe machinery.
/// </para>
/// </summary>
public static class ExplorerPluginAccessGates
{
    /// <summary>
    /// A gate that always allows. Use for a plugin with no capability of its
    /// own to probe - the tree browser, for example, which is reachable
    /// whenever the shell is.
    /// </summary>
    public static IExplorerPluginAccessGate Allowed { get; } = new FixedGate(ExplorerPluginAccess.Allowed);

    /// <summary>A gate that always denies. The fail-closed placeholder.</summary>
    public static IExplorerPluginAccessGate Denied { get; } = new FixedGate(ExplorerPluginAccess.Denied);

    /// <summary>A gate that always reports that a sign-in is required.</summary>
    public static IExplorerPluginAccessGate AuthenticationRequired { get; }
        = new FixedGate(ExplorerPluginAccess.AuthenticationRequired);

    /// <summary>
    /// A gate that always reports the capability absent, so the plugin degrades
    /// to nothing. Use when a head registers a plugin whose backing capability
    /// is known not to be installed.
    /// </summary>
    public static IExplorerPluginAccessGate Unavailable { get; }
        = new FixedGate(ExplorerPluginAccess.Unavailable);

    /// <summary>
    /// Returns a gate that always resolves to <paramref name="access"/>.
    /// </summary>
    /// <param name="access">The fixed decision to report.</param>
    public static IExplorerPluginAccessGate Fixed(ExplorerPluginAccess access) => access.State switch
    {
        ExplorerPluginAccessState.Allowed when access.Reason is null => Allowed,
        ExplorerPluginAccessState.Denied when access.Reason is null => Denied,
        ExplorerPluginAccessState.AuthenticationRequired when access.Reason is null => AuthenticationRequired,
        ExplorerPluginAccessState.Unavailable when access.Reason is null => Unavailable,
        _ => new FixedGate(access),
    };

    /// <summary>
    /// Returns a gate that runs <paramref name="probe"/>. The delegate is
    /// stored once, so the gate costs one allocation at construction and none
    /// per probe.
    /// </summary>
    /// <param name="probe">The probe to run. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="probe"/> is <see langword="null"/>.</exception>
    public static IExplorerPluginAccessGate FromDelegate(
        Func<IExplorerPluginHostContext, CancellationToken, ValueTask<ExplorerPluginAccess>> probe)
    {
        ArgumentNullException.ThrowIfNull(probe);
        return new DelegateGate(probe);
    }

    private sealed class FixedGate(ExplorerPluginAccess access) : IExplorerPluginAccessGate
    {
        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(context);
            return ValueTask.FromResult(access);
        }
    }

    private sealed class DelegateGate(
        Func<IExplorerPluginHostContext, CancellationToken, ValueTask<ExplorerPluginAccess>> probe)
        : IExplorerPluginAccessGate
    {
        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(context);
            return probe(context, cancellationToken);
        }
    }
}
