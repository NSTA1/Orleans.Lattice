using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Public API contract suite for materialised views. Every test exercises only
/// the public surface a caller depends on - <c>AddLatticeViews</c> /
/// <c>AddView</c> / <c>AddAggregationView</c> registration,
/// <see cref="ILatticeViewFactory"/>, and the <see cref="ILatticeView"/> read /
/// lag / rebuild / reconcile / digest / barrier methods - over a single silo that
/// never calls <c>AddLatticeReplication</c>. The suite therefore doubles as the
/// proof that a local (<see cref="LatticeViewReplicationMode.DeriveLocally"/>)
/// view needs a WAL provider, not a replicated cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewPublicApiContractTests
{
    private MaterialisedViewPublicApiContractFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewPublicApiContractFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(10);

    [Test]
    public async Task Local_view_converges_through_the_public_handle_without_replication()
    {
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(MaterialisedViewPublicApiContractFixture.FilterSourceTreeId);
        await source.SetAsync("a", MaterialisedViewPublicApiContractFixture.PersonBytes(30));
        await source.SetAsync("b", MaterialisedViewPublicApiContractFixture.PersonBytes(40));
        await source.SetAsync("c", MaterialisedViewPublicApiContractFixture.PersonBytes(10)); // filtered out

        var view = _fixture.FilterView();
        await view.WaitForSourceHeadAsync(Barrier);

        // The silo never called AddLatticeReplication, so replication is disabled
        // here - yet the DeriveLocally view still converges off the WAL.
        var replicationContext = _fixture.SiloServices.GetRequiredService<ILatticeReplicationContext>();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(replicationContext.IsReplicationEnabled, Is.False, "the contract fixture must not enable replication");
            Assert.That(await view.GetAsync("a"), Is.Not.Null, "an in-predicate source key should surface in the view");
            Assert.That(await view.GetAsync("c"), Is.Null, "an out-of-predicate source key must not surface");
            Assert.That(await view.CountAsync(), Is.EqualTo(2), "the view should hold exactly the in-predicate keys");
        });

        var keys = new List<string>();
        await foreach (var key in view.KeysAsync())
        {
            keys.Add(key);
        }

        Assert.That(keys, Is.EquivalentTo(new[] { "a", "b" }), "KeysAsync should stream the converged view keys");
    }

    [Test]
    public async Task GetLagAsync_reports_zero_once_caught_up()
    {
        const string tree = "people-lag";
        const string viewName = "adults-lag";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("lag-probe", MaterialisedViewPublicApiContractFixture.PersonBytes(25));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetLagAsync(), Is.Zero, "GetLagAsync should report zero after the view catches up to the source head");
    }

    [Test]
    public async Task RebuildAsync_reprojects_current_source_state()
    {
        const string tree = "people-rebuild";
        const string viewName = "adults-rebuild";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("x", MaterialisedViewPublicApiContractFixture.PersonBytes(50));
        await source.SetAsync("y", MaterialisedViewPublicApiContractFixture.PersonBytes(12)); // filtered out

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));

        await view.RebuildAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await view.GetAsync("x"), Is.Not.Null, "rebuild should materialise the in-predicate source key");
            Assert.That(await view.GetAsync("y"), Is.Null, "rebuild should exclude the out-of-predicate source key");
        });
    }

    [Test]
    public async Task ReconcileAsync_returns_false_when_the_view_matches_its_source()
    {
        const string tree = "people-reconcile";
        const string viewName = "adults-reconcile";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("reconcile-probe", MaterialisedViewPublicApiContractFixture.PersonBytes(33));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.ReconcileAsync(), Is.False, "a view already in sync with its source should report no drift");
    }

    [Test]
    public async Task ComputeDigestAsync_differs_for_different_view_contents()
    {
        const string tree = "people-digest";
        const string viewName = "adults-digest";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));

        await source.SetAsync("d1", MaterialisedViewPublicApiContractFixture.PersonBytes(20));
        await view.WaitForSourceHeadAsync(Barrier);
        var firstDigest = await view.ComputeDigestAsync();

        await source.SetAsync("d2", MaterialisedViewPublicApiContractFixture.PersonBytes(21));
        await view.WaitForSourceHeadAsync(Barrier);
        var secondDigest = await view.ComputeDigestAsync();

        Assert.That(secondDigest, Is.Not.EqualTo(firstDigest), "the digest must change when the materialised view content changes");
    }

    [Test]
    public async Task Direct_user_writes_and_reads_to_a_view_tree_are_rejected_while_the_handle_and_maintainer_still_work()
    {
        const string tree = "people-readonly";
        const string viewName = "adults-readonly";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("p1", MaterialisedViewPublicApiContractFixture.PersonBytes(40));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));
        await view.WaitForSourceHeadAsync(Barrier);

        // A caller could discover the underlying view tree id (view-{name}) and grab
        // its ILattice grain reference directly. Every public mutating call must be
        // rejected - the view is derived state owned by its maintainer - and every
        // public content read must be rejected too, because a rebuild can swap the
        // active generation underneath this fixed bind.
        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>("view-" + viewName);

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.SetAsync("p1", MaterialisedViewPublicApiContractFixture.PersonBytes(99)),
                "a direct SetAsync to a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.DeleteAsync("p1"),
                "a direct DeleteAsync to a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.SetManyAtomicAsync(
                    new List<KeyValuePair<string, byte[]>> { new("p1", MaterialisedViewPublicApiContractFixture.PersonBytes(1)) },
                    "op"),
                "a direct atomic write to a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.DeleteRangeAsync("a", "z"),
                "a direct DeleteRange to a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.GetAsync("p1"),
                "a direct GetAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(
                () => viewTree.CountAsync(),
                "a direct CountAsync against a view tree must be rejected");
        });

        // The content is still readable through the supported ILatticeView handle,
        // and the rejected writes left the view untouched.
        Assert.That(await view.GetAsync("p1"), Is.Not.Null, "reads through the view handle must remain allowed");

        // The maintainer still owns the view: a fresh source write converges normally.
        await source.SetAsync("p2", MaterialisedViewPublicApiContractFixture.PersonBytes(55));
        await view.WaitForSourceHeadAsync(Barrier);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await view.GetAsync("p2"), Is.Not.Null, "the maintainer must still apply source writes after a rejected direct write");
            Assert.That(await view.CountAsync(), Is.EqualTo(2), "the view should hold exactly the maintainer-applied keys");
        });
    }

    [Test]
    public async Task Aggregation_view_materialises_group_values_through_the_public_handle()
    {
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(MaterialisedViewPublicApiContractFixture.AggregationSourceTreeId);
        await source.SetAsync("o1", MaterialisedViewPublicApiContractFixture.OrderBytes("Alice", 10.5));
        await source.SetAsync("o2", MaterialisedViewPublicApiContractFixture.OrderBytes("Alice", 4.5));
        await source.SetAsync("o3", MaterialisedViewPublicApiContractFixture.OrderBytes("Bob", 7));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            MaterialisedViewPublicApiContractFixture.AggregationViewName,
            new LatticeViewDefinition(
                MaterialisedViewPublicApiContractFixture.AggregationViewName,
                MaterialisedViewPublicApiContractFixture.AmountByCustomer()));

        await view.WaitForSourceHeadAsync(Barrier);

        var alice = await view.GetAsync("Alice");
        var bob = await view.GetAsync("Bob");

        await Assert.MultipleAsync(() =>
        {
            Assert.That(alice, Is.Not.Null, "the aggregation view should materialise a value for a group with live members");
            Assert.That(LatticeAggregationValue.DecodeDouble(alice!), Is.EqualTo(15).Within(1e-9), "Alice's order amounts should sum");
            Assert.That(LatticeAggregationValue.DecodeDouble(bob!), Is.EqualTo(7).Within(1e-9), "Bob's single order should reduce to its amount");
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task GetAsync_returns_null_for_an_unregistered_view()
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();

        var handle = await factory.GetAsync("no-such-view-was-ever-created");

        Assert.That(handle, Is.Null, "GetAsync must return null for a view that was never created");
    }

    [Test]
    public async Task GetAsync_opens_a_working_read_handle_for_an_existing_runtime_view()
    {
        const string tree = "people-getasync";
        const string viewName = "adults-getasync";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("ga-adult", MaterialisedViewPublicApiContractFixture.PersonBytes(42));
        await source.SetAsync("ga-minor", MaterialisedViewPublicApiContractFixture.PersonBytes(9)); // filtered out

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();

        // Register the view once with its definition...
        var created = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));
        await created.WaitForSourceHeadAsync(Barrier);

        // ...then re-open it by name only, without re-supplying the source or projection.
        var reopened = await factory.GetAsync(viewName);

        Assert.That(reopened, Is.Not.Null, "GetAsync must resolve a registered runtime view by name");
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await reopened!.GetAsync("ga-adult"), Is.Not.Null, "the re-opened handle must read an in-predicate key");
            Assert.That(await reopened.GetAsync("ga-minor"), Is.Null, "the re-opened handle must exclude an out-of-predicate key");
            Assert.That(await reopened.CountAsync(), Is.EqualTo(1), "the re-opened handle must report the converged key count");
        });
    }

    [Test]
    public async Task GetAsync_resolves_a_startup_declared_view_without_a_prior_create()
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();

        // The "adults" view is declared at startup via AddLatticeViews; GetAsync
        // must resolve it from the declaration/catalog with no Create call here.
        // (Resolution only - this test must not write to the shared startup source,
        // which other tests assert exact counts over.)
        var handle = await factory.GetAsync(MaterialisedViewPublicApiContractFixture.FilterViewName);

        Assert.That(handle, Is.Not.Null, "GetAsync must resolve a startup-declared view by name");

        // A lag of ">= 0" is vacuous - the maintainer returns 0 for an unresolved
        // view too. Driving the handle to the source head and then asserting an
        // exact zero lag is what actually proves it is live and maintainer-backed,
        // and it is a read-only operation over the shared startup source.
        await handle!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await handle.GetLagAsync(), Is.Zero, "the resolved startup-view handle must be a live, maintainer-backed view that converges to the source head");
    }

    [Test]
    public async Task GetAsync_resolves_the_aggregation_flag_so_the_count_excludes_reserved_rows()
    {
        const string tree = "orders-getasync";
        const string viewName = "amount-by-customer-getasync";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("o1", MaterialisedViewPublicApiContractFixture.OrderBytes("Zoe", 3));
        await source.SetAsync("o2", MaterialisedViewPublicApiContractFixture.OrderBytes("Zoe", 4));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var definition = new LatticeViewDefinition(
            viewName,
            MaterialisedViewPublicApiContractFixture.AmountByCustomer());
        var created = factory.Create(
            source,
            viewName,
            MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        await created.WaitForSourceHeadAsync(Barrier);

        var reopened = await factory.GetAsync(viewName);

        Assert.That(reopened, Is.Not.Null, "GetAsync must resolve a registered aggregation view by name");
        await Assert.MultipleAsync(async () =>
        {
            // If the aggregation flag were lost, CountAsync would fall back to the
            // whole-tree count and include the reserved accumulator rows (> 1).
            Assert.That(await reopened!.CountAsync(), Is.EqualTo(1), "the re-opened aggregation handle must count only the materialised group, excluding reserved rows");
            Assert.That(LatticeAggregationValue.DecodeDouble((await reopened.GetAsync("Zoe"))!), Is.EqualTo(7).Within(1e-9), "the re-opened aggregation handle must read the reduced group value");
        });
    }

    [Test]
    public async Task Every_public_content_read_against_a_view_tree_is_rejected()
    {
        const string tree = "people-readguard";
        const string viewName = "adults-readguard";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("rg1", MaterialisedViewPublicApiContractFixture.PersonBytes(30));

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var view = factory.Create(
            source,
            viewName,
            new LatticeViewDefinition(viewName, MaterialisedViewPublicApiContractFixture.AdultFilter()));
        await view.WaitForSourceHeadAsync(Barrier);

        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>("view-" + viewName);

        await Assert.MultipleAsync(async () =>
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.GetAsync("rg1"), "GetAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.GetWithVersionAsync("rg1"), "GetWithVersionAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.ExistsAsync("rg1"), "ExistsAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.GetManyAsync(new List<string> { "rg1" }), "GetManyAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.CountAsync(), "CountAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(() => viewTree.CountPerShardAsync(), "CountPerShardAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in viewTree.KeysAsync()) { }
            }, "KeysAsync against a view tree must be rejected");
            Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in viewTree.EntriesAsync()) { }
            }, "EntriesAsync against a view tree must be rejected");
            await Task.CompletedTask;
        });
    }
}
