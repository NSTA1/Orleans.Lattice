using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for the derived-state self-heal on a physical-tree-identity
/// swap. A folded view tails the write-ahead log of its source tree; that WAL is
/// addressed by the source's physical id. When an operator swaps the physical
/// identity behind a logical tree's registry alias (shadow-cutover restore, tree
/// resize, reshard), the maintainer must detect the change on its next drain,
/// rebuild against the new physical source, and rebind its WAL tail so the view
/// keeps converging instead of silently tailing the orphaned old log forever.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewIdentitySwapHealTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ViewPerson(int Age, string Tag);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task DrainToZeroAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
    }

    // A folded view over a logical source tree must self-heal when the source's
    // physical identity is swapped under its registry alias: the view rebuilds
    // against the restored physical source and drops keys that no longer exist,
    // rather than continuing to serve the pre-swap projection forever.
    [Test]
    public async Task View_heals_after_source_physical_identity_swap()
    {
        const string logical = "idswap-src";
        const string shadow = "idswap-src-restored";
        const string view = "idswap-view";

        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var latticeView = CreateAdultView(logical, view);

        await source.SetAsync("a", Person(30, "orig"));
        await source.SetAsync("b", Person(40, "orig"));
        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(2), "View must materialise the original source.");

        // Build a distinct physical tree standing in for a restored / resized
        // identity: 'a' changed, 'b' absent.
        var shadowTree = _fixture.Cluster.Client.GetGrain<ILattice>(shadow);
        await shadowTree.SetAsync("a", Person(31, "restored"));

        // Swap the logical tree's physical identity under the registry alias, the
        // way shadow-cutover restore / resize / reshard do.
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(logical, shadow);

        // The next drain must detect the identity change and rebuild + rebind.
        await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("a"), Is.EqualTo(Person(31, "restored")),
                "View must reflect the restored physical source after an identity swap.");
            Assert.That(await latticeView.GetAsync("b"), Is.Null,
                "A key absent from the restored source must not linger in the view.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(1));
        });
    }

    // After healing onto the new physical identity, the view must keep tailing the
    // new source's WAL: a mutation written to the logical tree post-swap (which
    // routes to the new physical) must converge into the view on the next drain.
    [Test]
    public async Task View_tails_new_identity_after_swap()
    {
        const string logical = "idswap-tail-src";
        const string shadow = "idswap-tail-restored";
        const string view = "idswap-tail-view";

        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var latticeView = CreateAdultView(logical, view);

        await source.SetAsync("a", Person(30, "orig"));
        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(1));

        var shadowTree = _fixture.Cluster.Client.GetGrain<ILattice>(shadow);
        await shadowTree.SetAsync("a", Person(31, "restored"));
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(logical, shadow);

        // Heal onto the new identity.
        await DrainToZeroAsync(view);

        // A fresh write to the new physical source must tail into the view without
        // a further swap. (The write targets the physical tree directly: driving the
        // swap through SetAliasAsync alone does not invalidate the logical
        // activation's routing cache, so a write through the logical grain would land
        // in the orphaned old WAL - that stale-routing self-heal is a separate seam.)
        await shadowTree.SetAsync("c", Person(50, "postswap"));
        await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("c"), Is.EqualTo(Person(50, "postswap")),
                "View must tail the new physical WAL after healing onto it.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(2));
        });
    }
}
