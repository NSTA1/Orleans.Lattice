using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The head's tenant-scope refresher: the adapter that turns a tenant switch
/// into the same pair of steps the shell already runs on mount, on a sign-in
/// change, and on a reconnect - re-project the tenant scope, then re-probe every
/// plugin gate.
/// </summary>
[TestFixture]
public sealed class ExplorerPluginTenantScopeRefresherTests
{
    [Test]
    public void Constructor_null_host_state_throws()
    {
        Assert.That(
            () => new ExplorerPluginTenantScopeRefresher(
                null!,
                () => Substitute.For<IExplorerPluginAccessRefresher>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_gate_refresher_throws()
    {
        using var state = HostState(out _);

        Assert.That(
            () => new ExplorerPluginTenantScopeRefresher(state, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task RefreshAsync_resolves_the_gate_refresher_only_when_a_refresh_runs()
    {
        // The deferred accessor is what keeps the container acyclic: the gate
        // refresher's graph reaches every plugin, and a plugin may depend on the
        // tenant switcher this type is notified by. Constructing the adapter must
        // therefore touch nothing.
        using var state = HostState(out _);
        var resolved = 0;
        var gates = Substitute.For<IExplorerPluginAccessRefresher>();
        gates.RefreshAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var refresher = new ExplorerPluginTenantScopeRefresher(
            state,
            () =>
            {
                resolved++;
                return gates;
            });

        Assert.That(resolved, Is.Zero, "construction must not resolve the gate refresher");

        await refresher.RefreshAsync();

        Assert.That(resolved, Is.EqualTo(1));
    }

    [Test]
    public async Task RefreshAsync_republishes_the_tenant_scope_and_reprobes_every_gate()
    {
        using var state = HostState(out var tenants);
        tenants.IsActive.Returns(true);
        tenants.ActiveTenant.Returns(new ExplorerTenantId("globex"));
        tenants
            .ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ExplorerTenantVisibility>(ExplorerTenantVisibility.AllTenants));

        var gates = Substitute.For<IExplorerPluginAccessRefresher>();
        gates.RefreshAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        await new ExplorerPluginTenantScopeRefresher(state, () => gates).RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.Tenant.ActiveTenantId, Is.EqualTo("globex"));
            Assert.That(
                state.Tenant.Visibility,
                Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
        });

        await gates.Received(1).RefreshAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RefreshAsync_publishes_the_scope_before_it_probes_the_gates()
    {
        // Ordering is load-bearing: a gate that reads the projected scope must
        // decide against the tenant the caller switched to, not the one they
        // left.
        using var state = HostState(out var tenants);
        tenants.IsActive.Returns(true);
        tenants.ActiveTenant.Returns(new ExplorerTenantId("globex"));
        tenants
            .ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ExplorerTenantVisibility>(ExplorerTenantVisibility.AllTenants));

        ExplorerPluginTenantScope? scopeSeenByGates = null;
        var gates = Substitute.For<IExplorerPluginAccessRefresher>();
        gates
            .RefreshAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                scopeSeenByGates = state.Tenant;
                return Task.CompletedTask;
            });

        await new ExplorerPluginTenantScopeRefresher(state, () => gates).RefreshAsync();

        Assert.That(scopeSeenByGates!.Value.ActiveTenantId, Is.EqualTo("globex"));
        Assert.That(
            scopeSeenByGates.Value.Visibility,
            Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
    }

    [Test]
    public async Task RefreshAsync_still_probes_the_gates_when_the_tenant_view_faults()
    {
        // The host state contains a failed resolution and degrades to the
        // fail-closed scope, so the gate probes must still run.
        using var state = HostState(out var tenants);
        tenants.IsActive.Returns(true);
        tenants
            .ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns<ExplorerTenantVisibility>(_ => throw new InvalidOperationException("no tenant service"));

        var gates = Substitute.For<IExplorerPluginAccessRefresher>();
        gates.RefreshAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        await new ExplorerPluginTenantScopeRefresher(state, () => gates).RefreshAsync();

        Assert.That(
            state.Tenant.Visibility,
            Is.EqualTo(ExplorerPluginTenantVisibility.ActiveTenant),
            "an unresolvable visibility is never an admission");
        await gates.Received(1).RefreshAsync(Arg.Any<CancellationToken>());
    }

    private static ExplorerPluginHostState HostState(out IExplorerTenantView tenants)
    {
        var selection = Substitute.For<IExplorerSelection>();
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        tenants = Substitute.For<IExplorerTenantView>();
        tenants
            .ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ExplorerTenantVisibility>(ExplorerTenantVisibility.ActiveTenant));
        return new ExplorerPluginHostState(selection, connection, tenants);
    }
}
