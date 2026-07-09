using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage of the materialised-view maintainer's self-heal when a folded
/// view's source tree has its PHYSICAL identity repointed under its logical
/// registry alias (a restore-style cutover) repeatedly, under a sustained
/// mutation workload. Complements the deterministic single-swap regression that
/// landed with the heal fix by driving many cutovers with backlog accumulating
/// between drains.
/// </summary>
/// <remarks>
/// A folded view tails the write-ahead log of its source, addressed by the
/// source's physical id. On each drain the maintainer re-resolves the logical
/// source to its current physical id; when that changes it rebuilds against the
/// new physical source and rebinds its tail. This suite pins that the view
/// converges to EXACTLY the final identity's contents after a burst of swaps -
/// keys dropped by a cutover are retracted (a WAL tail alone can never retract a
/// key the restored source never had), changed values win, and post-swap
/// mutations fold in - rather than silently tailing an orphaned old log.
/// <para>
/// Writes after a cutover target the new physical tree directly: driving the
/// swap through <c>SetAliasAsync</c> alone does not invalidate the logical
/// activation's routing cache, so a write through the logical grain would land
/// in the orphaned old WAL. That stale-routing self-heal is a separate seam.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class DerivedStateRecoveryAcrossIdentitySwapChaosTests
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
        for (var attempt = 0; attempt < 200; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0) return;
            await Task.Delay(20);
        }
        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
    }

    // Repeated restore-style cutovers, each dropping some adults, changing
    // others, and adding a generation marker, interleaved with a sustained
    // mutation burst on the identity being abandoned. After the final drain the
    // view must reflect EXACTLY the final identity - no stale rows from any
    // abandoned identity, all final values, correct count.
    [Test]
    public async Task View_converges_on_final_identity_after_repeated_cutovers_under_load()
    {
        const string logical = "chaos-mv-repeat-src";
        const string view = "chaos-mv-repeat-view";

        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var latticeView = CreateAdultView(logical, view);

        // Seed a baseline of adults (a00..a11) plus a couple of minors that the
        // filter excludes, then materialise.
        for (var i = 0; i < 12; i++) await source.SetAsync($"a{i:D2}", Person(20 + i, "orig"));
        await source.SetAsync("minor", Person(10, "orig"));
        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(12), "View must materialise the original adults.");

        ILattice current = source;
        for (var gen = 1; gen <= 4; gen++)
        {
            // Sustained mutation on the identity about to be abandoned: these
            // writes accumulate backlog the maintainer would tail if it failed
            // to detect the swap, so they must NOT survive the cutover.
            for (var i = 0; i < 6; i++) await current.SetAsync($"doomed-g{gen}-{i:D2}", Person(40 + i, $"doomed{gen}"));

            // Mint the next identity: keep the even-indexed adults (changed
            // value), drop the odd-indexed ones, and add a generation marker.
            var shadowId = $"{logical}-gen{gen}";
            await registry.RegisterAsync(shadowId, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });
            var shadow = _fixture.Cluster.Client.GetGrain<ILattice>(shadowId);
            for (var i = 0; i < 12; i += 2) await shadow.SetAsync($"a{i:D2}", Person(50 + gen, $"gen{gen}"));
            await shadow.SetAsync($"gen-marker", Person(99, $"gen{gen}"));
            await registry.SetAliasAsync(logical, shadowId);

            // Drain intermittently so the rebuild happens under accumulated lag.
            await DrainToZeroAsync(view);
            current = shadow;
        }

        // Final identity holds: a00,a02,a04,a06,a08,a10 (6 adults) + gen-marker = 7.
        await DrainToZeroAsync(view);
        var expectedKept = Enumerable.Range(0, 12).Where(i => i % 2 == 0).Select(i => $"a{i:D2}").ToArray();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(expectedKept.Length + 1),
                "View must reflect exactly the final identity's adult set plus the marker.");
            foreach (var k in expectedKept)
            {
                Assert.That(await latticeView.GetAsync(k), Is.EqualTo(Person(50 + 4, "gen4")),
                    $"Kept adult '{k}' must carry the final identity's value.");
            }
            for (var i = 1; i < 12; i += 2)
            {
                Assert.That(await latticeView.GetAsync($"a{i:D2}"), Is.Null,
                    $"Dropped adult 'a{i:D2}' must be retracted by the cutover rebuild.");
            }
            Assert.That(await latticeView.GetAsync("gen-marker"), Is.EqualTo(Person(99, "gen4")));
        });
    }

    // A single cutover after a large accumulated backlog: the maintainer must
    // rebuild against the new identity and retract every key the restored source
    // never had, even when the pre-swap identity had grown far larger.
    [Test]
    public async Task View_rebuilds_against_new_identity_after_large_backlog_cutover()
    {
        const string logical = "chaos-mv-backlog-src";
        const string view = "chaos-mv-backlog-view";

        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var latticeView = CreateAdultView(logical, view);

        // Grow a large source, materialise it, then pile on more backlog without
        // draining so the swap is detected with a deep un-drained tail.
        for (var i = 0; i < 40; i++) await source.SetAsync($"k{i:D3}", Person(18 + (i % 30), "orig"));
        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(40));
        for (var i = 40; i < 80; i++) await source.SetAsync($"k{i:D3}", Person(18 + (i % 30), "extra"));

        // Restored identity is small: only k000..k004 survive, with new values.
        var shadowId = $"{logical}-restored";
        await registry.RegisterAsync(shadowId, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });
        var shadow = _fixture.Cluster.Client.GetGrain<ILattice>(shadowId);
        for (var i = 0; i < 5; i++) await shadow.SetAsync($"k{i:D3}", Person(60, "restored"));
        await registry.SetAliasAsync(logical, shadowId);

        await DrainToZeroAsync(view);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(5),
                "View must shrink to the restored identity's small set.");
            for (var i = 0; i < 5; i++)
            {
                Assert.That(await latticeView.GetAsync($"k{i:D3}"), Is.EqualTo(Person(60, "restored")));
            }
            Assert.That(await latticeView.GetAsync("k050"), Is.Null,
                "A key present only in the abandoned identity must not linger in the view.");
        });
    }
}
