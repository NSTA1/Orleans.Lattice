using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// Builds a <see cref="MyTenantWorkspace"/> over a scripted
/// <see cref="FakeTenancyDomain"/> and a real
/// <see cref="ExplorerPluginAccessStore"/>, with the plugin's gate decision
/// already filed.
/// <para>
/// The real store is used rather than a stub because the workspace's fail-closed
/// posture is a property of reading it: an unprobed key reads as denied, and a
/// test that stubbed the store could not observe that.
/// </para>
/// </summary>
internal sealed class MyTenantWorkspaceHarness
{
    private MyTenantWorkspaceHarness(
        FakeTenancyDomain domain,
        IExplorerPluginAccessStore store,
        MyTenantWorkspace workspace)
    {
        Domain = domain;
        Store = store;
        Workspace = workspace;
    }

    /// <summary>The scripted domain the workspace operates against.</summary>
    public FakeTenancyDomain Domain { get; }

    /// <summary>The operations surface, for asserting what actually left the process.</summary>
    public FakeTenantAdminService Service => Domain.Service;

    /// <summary>The keyed access store the gate decision is filed in.</summary>
    public IExplorerPluginAccessStore Store { get; }

    /// <summary>The workspace under test.</summary>
    public MyTenantWorkspace Workspace { get; }

    /// <summary>
    /// Creates a harness whose gate has already admitted the caller, and awaits
    /// the workspace's initial load so a test starts from a loaded surface.
    /// </summary>
    /// <param name="configure">An optional script applied before the load.</param>
    /// <param name="access">The gate decision to file, defaulting to allowed.</param>
    /// <param name="operatorGateDiagnostic">
    /// An optional registration-order diagnostic to file, as the plugin's gate
    /// would on a misordered head.
    /// </param>
    public static async Task<MyTenantWorkspaceHarness> CreateAsync(
        Action<FakeTenancyDomain>? configure = null,
        ExplorerPluginAccess? access = null,
        string? operatorGateDiagnostic = null)
    {
        var harness = Create(configure, access, operatorGateDiagnostic);
        await harness.Workspace.InitializeAsync();
        return harness;
    }

    /// <summary>
    /// Creates a harness without loading it, for a test that needs to observe
    /// the pre-load state.
    /// </summary>
    /// <param name="configure">An optional script applied before construction.</param>
    /// <param name="access">The gate decision to file, defaulting to allowed.</param>
    /// <param name="operatorGateDiagnostic">
    /// An optional registration-order diagnostic to file.
    /// </param>
    public static MyTenantWorkspaceHarness Create(
        Action<FakeTenancyDomain>? configure = null,
        ExplorerPluginAccess? access = null,
        string? operatorGateDiagnostic = null)
    {
        var domain = new FakeTenancyDomain();
        configure?.Invoke(domain);

        var store = new ExplorerPluginAccessStore();
        store.Set(MyTenantPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);
        store.Set(
            MyTenantPluginKeys.PluginId,
            MyTenantPluginKeys.OperatorGateScope,
            operatorGateDiagnostic is null
                ? ExplorerPluginAccess.Allowed
                : ExplorerPluginAccess.Deny(operatorGateDiagnostic));

        return new MyTenantWorkspaceHarness(domain, store, new MyTenantWorkspace(domain, store));
    }

    /// <summary>Activates a sub-surface and awaits its load.</summary>
    /// <param name="surfaceId">The surface to activate.</param>
    public Task OpenAsync(string surfaceId) => Workspace.SelectSurfaceAsync(surfaceId);
}
