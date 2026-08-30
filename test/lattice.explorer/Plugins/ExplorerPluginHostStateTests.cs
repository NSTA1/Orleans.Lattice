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

    [Test]
    public async Task A_stale_tenant_scope_resolution_does_not_widen_a_newer_one()
    {
        // Two scope refreshes overlap - the shell fires one on an authentication
        // change and another when the connection reaches the cluster, both
        // fire-and-forget - and the older one resolves last. The scope published
        // must be the one asked for last: a resolution issued while the caller
        // still validated as a platform operator must not restore the
        // cross-tenant view after a newer resolution narrowed it.
        var tenants = new SequencedTenantView();
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);

        var stale = state.RefreshTenantScopeAsync();
        var fresh = state.RefreshTenantScopeAsync();

        // Await each resolution before releasing the next. Completing both and
        // then awaiting together would leave the two continuations racing on the
        // thread pool, so "the stale one lands second" would be a hope rather
        // than a fact - see SequencedTenantView.
        tenants.Complete(1, ExplorerTenantVisibility.ActiveTenant);
        await fresh;
        tenants.Complete(0, ExplorerTenantVisibility.AllTenants);
        await stale;

        Assert.That(
            state.Tenant.Visibility,
            Is.EqualTo(ExplorerPluginTenantVisibility.ActiveTenant),
            "a stale resolution must never widen the scope a newer one narrowed");
    }

    [Test]
    public async Task A_stale_tenant_scope_resolution_raises_no_transition()
    {
        // The discarded resolution must not announce itself either: a plugin
        // that re-reads the scope on ExplorerPluginHostChange.Tenant would
        // otherwise be told to re-render into a scope the host never adopted.
        var tenants = new SequencedTenantView();
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);
        var changes = new List<ExplorerPluginHostChange>();
        state.Changed += changes.Add;

        var stale = state.RefreshTenantScopeAsync();
        var fresh = state.RefreshTenantScopeAsync();
        tenants.Complete(1, ExplorerTenantVisibility.ActiveTenant);
        await fresh;
        tenants.Complete(0, ExplorerTenantVisibility.AllTenants);
        await stale;

        Assert.That(changes, Is.Empty, "the newer resolution matched the projected scope, so nothing changed");
    }

    [Test]
    public async Task A_newer_tenant_scope_resolution_still_applies_when_it_resolves_last()
    {
        // The ordering guard must not degrade into "first answer wins": when the
        // resolutions complete in request order the newest is still adopted.
        var tenants = new SequencedTenantView();
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);

        var older = state.RefreshTenantScopeAsync();
        var newer = state.RefreshTenantScopeAsync();

        tenants.Complete(0, ExplorerTenantVisibility.ActiveTenant);
        await older;
        tenants.Complete(1, ExplorerTenantVisibility.AllTenants);
        await newer;

        Assert.That(state.Tenant.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
    }

    [Test]
    public async Task A_refresh_after_a_discarded_resolution_is_still_applied()
    {
        // The guard orders publications, it does not latch one: a later refresh
        // must still be able to widen the scope once the caller validates again.
        var tenants = new SequencedTenantView();
        using var state = new ExplorerPluginHostState(
            Substitute.For<IExplorerSelection>(),
            DisconnectedConnection(),
            tenants);

        var stale = state.RefreshTenantScopeAsync();
        var fresh = state.RefreshTenantScopeAsync();
        tenants.Complete(1, ExplorerTenantVisibility.ActiveTenant);
        await fresh;
        tenants.Complete(0, ExplorerTenantVisibility.AllTenants);
        await stale;

        var later = state.RefreshTenantScopeAsync();
        tenants.Complete(2, ExplorerTenantVisibility.AllTenants);
        await later;

        Assert.That(state.Tenant.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
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

    /// <summary>
    /// A tenant view that hands every visibility resolution its own pending
    /// completion source, so a test can answer overlapping resolutions in any
    /// order it likes without a clock.
    /// </summary>
    /// <summary>
    /// A tenant view whose resolutions are completed explicitly by index, so a
    /// test can drive two overlapping refreshes to resolve in a chosen order.
    /// </summary>
    /// <remarks>
    /// Resolutions use <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>,
    /// so completing one schedules the awaiting refresh's continuation on the
    /// thread pool rather than running it inline. Completing two resolutions
    /// back to back therefore leaves their continuations racing, and the order
    /// they publish in is not the order they were completed in. A test that
    /// depends on one landing before the other must complete a resolution and
    /// <b>await that refresh</b> before completing the next; awaiting both
    /// together with <see cref="Task.WhenAll(Task[])"/> does not order them and
    /// flakes under load.
    /// </remarks>
    private sealed class SequencedTenantView : IExplorerTenantView
    {
        private readonly List<TaskCompletionSource<ExplorerTenantVisibility>> _resolutions = [];

        public bool IsActive => true;

        public ExplorerTenantId? ActiveTenant => new("acme");

        /// <summary>Completes the <paramref name="index"/>th resolution with <paramref name="visibility"/>.</summary>
        public void Complete(int index, ExplorerTenantVisibility visibility)
        {
            TaskCompletionSource<ExplorerTenantVisibility> resolution;
            lock (_resolutions)
            {
                resolution = _resolutions[index];
            }

            resolution.SetResult(visibility);
        }

        public ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(
            CancellationToken cancellationToken = default)
        {
            var resolution = new TaskCompletionSource<ExplorerTenantVisibility>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_resolutions)
            {
                _resolutions.Add(resolution);
            }

            return new ValueTask<ExplorerTenantVisibility>(resolution.Task);
        }

        public bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId) => true;

        public ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
            IReadOnlyList<TItem> items,
            Func<TItem, string> treeIdSelector,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(items);
    }
}
