using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// A settable <see cref="IExplorerPluginHostState"/> for the plugin contract
/// tests. Transitions are raised explicitly by the test, so nothing here
/// depends on timing.
/// </summary>
internal sealed class FakeExplorerPluginHostState : IExplorerPluginHostState
{
    public ExplorerPluginSelection? Selection { get; set; }

    public ExplorerPluginConnectionStatus Connection { get; set; } =
        ExplorerPluginConnectionStatus.Disconnected;

    public ExplorerPluginTenantScope Tenant { get; set; } = ExplorerPluginTenantScope.Inactive;

    public event Action<ExplorerPluginHostChange>? Changed;

    /// <summary>The number of handlers currently subscribed to <see cref="Changed"/>.</summary>
    public int SubscriberCount => Changed?.GetInvocationList().Length ?? 0;

    /// <summary>Raises <see cref="Changed"/> for <paramref name="change"/>.</summary>
    public void Raise(ExplorerPluginHostChange change) => Changed?.Invoke(change);
}
