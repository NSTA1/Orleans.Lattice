using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Builds a <see cref="TelemetryWorkspace"/> over a scripted
/// <see cref="FakeExplorerTelemetryDomain"/>, a real
/// <see cref="ExplorerPluginAccessStore"/>, and a clock pinned to
/// <see cref="ExplorerTelemetrySample.Now"/>.
/// <para>
/// The real store is used rather than a stub because the workspace's fail-closed
/// posture is a property of reading it: an unprobed key reads as denied, and a
/// test that stubbed the store could not observe that.
/// </para>
/// <para>
/// The clock is pinned rather than real, so every assertion about the window a
/// request carried is exact rather than a tolerance around wall-clock time.
/// </para>
/// </summary>
internal sealed class TelemetryWorkspaceHarness : IDisposable
{
    private TelemetryWorkspaceHarness(
        FakeExplorerTelemetryDomain domain,
        ExplorerPluginAccessStore store,
        TelemetryWorkspace workspace)
    {
        Domain = domain;
        Store = store;
        Workspace = workspace;
        Workspace.Changed += () => ChangeCount++;
    }

    /// <summary>The scripted domain the workspace operates against.</summary>
    public FakeExplorerTelemetryDomain Domain { get; }

    /// <summary>The keyed access store the gate decision is filed in.</summary>
    public ExplorerPluginAccessStore Store { get; }

    /// <summary>The workspace under test.</summary>
    public TelemetryWorkspace Workspace { get; }

    /// <summary>How many times the workspace announced a change.</summary>
    public int ChangeCount { get; private set; }

    /// <summary>
    /// Creates a harness and awaits its initial load, so a test starts from a
    /// discovered catalogue and a rendered chart.
    /// </summary>
    /// <param name="configure">An optional script applied before the load.</param>
    /// <param name="access">The gate decision to file, defaulting to allowed.</param>
    /// <param name="pinnedToOwnTenant">Whether to build the tenant-pinned mount.</param>
    public static async Task<TelemetryWorkspaceHarness> CreateAsync(
        Action<FakeExplorerTelemetryDomain>? configure = null,
        ExplorerPluginAccess? access = null,
        bool pinnedToOwnTenant = false)
    {
        var harness = Create(configure, access, pinnedToOwnTenant);
        await harness.Workspace.InitializeAsync();
        return harness;
    }

    /// <summary>
    /// Creates a harness without loading it, for a test that needs to observe
    /// the pre-load state.
    /// </summary>
    /// <param name="configure">An optional script applied before construction.</param>
    /// <param name="access">The gate decision to file, defaulting to allowed.</param>
    /// <param name="pinnedToOwnTenant">Whether to build the tenant-pinned mount.</param>
    public static TelemetryWorkspaceHarness Create(
        Action<FakeExplorerTelemetryDomain>? configure = null,
        ExplorerPluginAccess? access = null,
        bool pinnedToOwnTenant = false)
    {
        var domain = new FakeExplorerTelemetryDomain();
        configure?.Invoke(domain);

        var store = new ExplorerPluginAccessStore();
        store.Set(TelemetryPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        var clock = new FixedTimeProvider(ExplorerTelemetrySample.Now);

        return new TelemetryWorkspaceHarness(
            domain,
            store,
            new TelemetryWorkspace(domain, store, clock, pinnedToOwnTenant));
    }

    /// <inheritdoc />
    public void Dispose() => Workspace.Dispose();

    /// <summary>
    /// A clock that does not move. Hand-rolled rather than taken from a testing
    /// package, so the test project acquires no dependency for one member, and
    /// so no test can accidentally advance it and reintroduce the wall-clock
    /// dependence the injected clock exists to remove.
    /// </summary>
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
