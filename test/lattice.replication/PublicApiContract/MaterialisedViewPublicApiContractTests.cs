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
    public async Task Direct_user_writes_to_a_view_tree_are_rejected_while_reads_and_the_maintainer_still_work()
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
        // rejected - the view is derived state owned by its maintainer.
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
        });

        // Reads against the view tree are unaffected, and the rejected writes left
        // the view untouched.
        Assert.That(await viewTree.GetAsync("p1"), Is.Not.Null, "reads on a view tree must remain allowed");

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
}
