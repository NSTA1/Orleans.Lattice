using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The head-supplied ambient-state adapter. The plugin contract deliberately
/// carries no cluster dependency, so it cannot implement this itself; this is
/// the shell's side of that seam, and it must project the Explorer's own
/// selection, connection and tenant view onto the contract's narrow shapes
/// without ever handing a plugin the services behind them.
/// </summary>
[TestFixture]
public sealed class ExplorerPluginHostStateTests
{
    [Test]
    public void Constructor_null_selection_throws()
    {
        Assert.That(
            () => new ExplorerPluginHostState(null!, Substitute.For<ILatticeStateConnection>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_connection_throws()
    {
        Assert.That(
            () => new ExplorerPluginHostState(Substitute.For<IExplorerSelection>(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void No_selection_projects_to_null()
    {
        using var state = Build(out _, out _);

        Assert.That(state.Selection, Is.Null);
    }

    [Test]
    public void A_selected_tree_projects_its_id_label_and_kind()
    {
        using var state = Build(out var selection, out _);
        Select(state, selection, new CatalogItem { Id = "orders", Kind = CatalogKind.Trees });

        Assert.Multiple(() =>
        {
            Assert.That(state.Selection!.Id, Is.EqualTo("orders"));
            Assert.That(state.Selection.Label, Is.EqualTo("orders"));
            Assert.That(state.Selection.Kind, Is.EqualTo(ExplorerPluginSelectionKind.Tree));
        });
    }

    [Test]
    public void A_selected_view_projects_its_display_name_as_the_label()
    {
        using var state = Build(out var selection, out _);
        Select(
            state,
            selection,
            new CatalogItem { Id = "view-orders", DisplayName = "orders", Kind = CatalogKind.Views });

        Assert.Multiple(() =>
        {
            Assert.That(state.Selection!.Id, Is.EqualTo("view-orders"));
            Assert.That(state.Selection.Label, Is.EqualTo("orders"));
            Assert.That(state.Selection.Kind, Is.EqualTo(ExplorerPluginSelectionKind.View));
        });
    }

    [Test]
    public void A_selected_tag_index_projects_the_tag_index_kind()
    {
        using var state = Build(out var selection, out _);
        Select(state, selection, new CatalogItem { Id = "tag-colour", Kind = CatalogKind.TagIndexes });

        Assert.That(state.Selection!.Kind, Is.EqualTo(ExplorerPluginSelectionKind.TagIndex));
    }

    [Test]
    public void A_selection_change_raises_exactly_the_selection_transition()
    {
        using var state = Build(out var selection, out _);
        var changes = new List<ExplorerPluginHostChange>();
        state.Changed += changes.Add;

        Select(state, selection, new CatalogItem { Id = "orders", Kind = CatalogKind.Trees });

        Assert.That(changes, Is.EqualTo(new[] { ExplorerPluginHostChange.Selection }));
    }

    [Test]
    public void Re_selecting_the_same_item_raises_nothing()
    {
        using var state = Build(out var selection, out _);
        Select(state, selection, new CatalogItem { Id = "orders", Kind = CatalogKind.Trees });
        var changes = 0;
        state.Changed += _ => changes++;

        Select(state, selection, new CatalogItem { Id = "orders", Kind = CatalogKind.Trees });

        Assert.That(changes, Is.Zero, "an unchanged projection must not re-render the shell");
    }

    [Test]
    public void The_initial_connection_status_is_projected_at_construction()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(new LatticeConnectionStatus(LatticeConnectionState.Connected, "a", null));
        using var state = new ExplorerPluginHostState(Substitute.For<IExplorerSelection>(), connection);

        Assert.That(state.Connection.State, Is.EqualTo(ExplorerPluginConnectionState.Connected));
    }

    [TestCase(LatticeConnectionState.Disconnected, ExplorerPluginConnectionState.Disconnected)]
    [TestCase(LatticeConnectionState.Connecting, ExplorerPluginConnectionState.Connecting)]
    [TestCase(LatticeConnectionState.Connected, ExplorerPluginConnectionState.Connected)]
    [TestCase(LatticeConnectionState.Reconnecting, ExplorerPluginConnectionState.Reconnecting)]
    [TestCase(LatticeConnectionState.Faulted, ExplorerPluginConnectionState.Faulted)]
    public void Every_connection_state_projects_onto_its_counterpart(
        LatticeConnectionState source,
        ExplorerPluginConnectionState expected)
    {
        using var state = Build(out _, out var connection);

        RaiseStatus(state, connection, new LatticeConnectionStatus(source, "a", null));

        Assert.That(state.Connection.State, Is.EqualTo(expected));
    }

    [Test]
    public void The_authentication_required_flag_crosses_the_seam()
    {
        using var state = Build(out _, out var connection);

        RaiseStatus(
            state,
            connection,
            new LatticeConnectionStatus(LatticeConnectionState.Faulted, "a", "no token", RequiresAuthentication: true));

        Assert.That(state.Connection.RequiresAuthentication, Is.True);
    }

    [Test]
    public void A_connection_change_that_alters_nothing_a_plugin_reads_raises_nothing()
    {
        using var state = Build(out _, out var connection);
        RaiseStatus(state, connection, new LatticeConnectionStatus(LatticeConnectionState.Connected, "a", "one"));
        var changes = 0;
        state.Changed += _ => changes++;

        // The endpoint and message are not part of the projection, so a status
        // that differs only in those must not churn every plugin.
        RaiseStatus(state, connection, new LatticeConnectionStatus(LatticeConnectionState.Connected, "b", "two"));

        Assert.That(changes, Is.Zero);
    }

    [Test]
    public void With_no_tenant_view_the_scope_is_inactive()
    {
        using var state = Build(out _, out _);

        Assert.Multiple(() =>
        {
            Assert.That(state.Tenant.IsActive, Is.False);
            Assert.That(state.Tenant.ActiveTenantId, Is.Null);
            Assert.That(state.Tenant.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.ActiveTenant));
        });
    }

    [Test]
    public async Task An_active_tenant_view_projects_its_tenant_and_resolved_visibility()
    {
        var tenants = Substitute.For<IExplorerTenantView>();
        tenants.IsActive.Returns(true);
        tenants.ActiveTenant.Returns(new ExplorerTenantId("acme"));
        tenants.ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ExplorerTenantVisibility>(ExplorerTenantVisibility.AllTenants));
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);

        await state.RefreshTenantScopeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.Tenant.IsActive, Is.True);
            Assert.That(state.Tenant.ActiveTenantId, Is.EqualTo("acme"));
            Assert.That(state.Tenant.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
        });
    }

    [Test]
    public async Task A_tenant_view_that_throws_degrades_to_the_active_tenant_scope()
    {
        var tenants = Substitute.For<IExplorerTenantView>();
        tenants.IsActive.Returns(true);
        tenants.ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns<ValueTask<ExplorerTenantVisibility>>(_ => throw new InvalidOperationException("boom"));
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);

        await state.RefreshTenantScopeAsync();

        Assert.That(
            state.Tenant.Visibility,
            Is.EqualTo(ExplorerPluginTenantVisibility.ActiveTenant),
            "an unresolvable visibility is never an admission");
    }

    [Test]
    public async Task A_tenant_scope_change_raises_exactly_the_tenant_transition()
    {
        var tenants = Substitute.For<IExplorerTenantView>();
        tenants.IsActive.Returns(true);
        tenants.ResolveEffectiveVisibilityAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ExplorerTenantVisibility>(ExplorerTenantVisibility.AllTenants));
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);
        var changes = new List<ExplorerPluginHostChange>();
        state.Changed += changes.Add;

        await state.RefreshTenantScopeAsync();
        await state.RefreshTenantScopeAsync();

        Assert.That(
            changes,
            Is.EqualTo(new[] { ExplorerPluginHostChange.Tenant }),
            "the second refresh resolves the same scope, so it raises nothing");
    }

    [Test]
    public void Dispose_unsubscribes_from_the_explorer_services()
    {
        var state = Build(out var selection, out var connection);
        var changes = 0;
        state.Changed += _ => changes++;

        state.Dispose();
        Select(state, selection, new CatalogItem { Id = "orders", Kind = CatalogKind.Trees });
        RaiseStatus(state, connection, new LatticeConnectionStatus(LatticeConnectionState.Connected, "a", null));

        Assert.That(changes, Is.Zero);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var state = Build(out _, out _);
        state.Dispose();

        Assert.That(state.Dispose, Throws.Nothing);
    }

    private static ILatticeStateConnection DisconnectedConnection()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        return connection;
    }

    private static ExplorerPluginHostState Build(
        out IExplorerSelection selection,
        out ILatticeStateConnection connection)
    {
        selection = Substitute.For<IExplorerSelection>();
        connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        return new ExplorerPluginHostState(selection, connection);
    }

    private static void Select(ExplorerPluginHostState state, IExplorerSelection selection, CatalogItem item)
    {
        selection.Selected.Returns(item);
        selection.SelectionChanged += NSubstitute.Raise.Event<Action>();
    }

    private static void RaiseStatus(
        ExplorerPluginHostState state,
        ILatticeStateConnection connection,
        LatticeConnectionStatus status)
    {
        connection.Status.Returns(status);
        connection.StatusChanged += NSubstitute.Raise.Event<Action<LatticeConnectionStatus>>(status);
    }
}
