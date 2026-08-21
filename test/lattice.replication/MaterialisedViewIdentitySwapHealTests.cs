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

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName, ILatticeViewProjection projection)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var definition = new LatticeViewDefinition(viewName, projection);
        return factory.Create(
            source,
            viewName,
            MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
    }

    // A projection that fails its next N Project calls, then delegates to a real
    // predicate projection. Arming it just before a heal makes the identity-swap
    // rebuild throw deterministically, standing in for the transient source-scan
    // abort (enumerator expiry) that the live cluster hit.
    private sealed class FaultInjectingProjection(ILatticeViewProjection inner) : ILatticeViewProjection
    {
        private int _failuresRemaining;

        public void ArmFailures(int count) => Volatile.Write(ref _failuresRemaining, count);

        public string ProjectionVersion => inner.ProjectionVersion;

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            while (true)
            {
                var remaining = Volatile.Read(ref _failuresRemaining);
                if (remaining <= 0)
                {
                    break;
                }

                if (Interlocked.CompareExchange(ref _failuresRemaining, remaining - 1, remaining) == remaining)
                {
                    throw new InvalidOperationException("injected rebuild failure");
                }
            }

            return inner.Project(mutation);
        }
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

    // A rebuild that throws during the identity-swap heal must NOT advance the
    // recorded binding: the maintainer must retry the heal on the next drain until
    // the rebuild succeeds. Regression for a heal that persisted the new physical
    // binding before rebuilding - a rebuild failure then latched the view onto the
    // new identity with a satisfied equality check, so the swap was never re-healed
    // and the view served the pre-swap projection forever (only its tail advanced,
    // which can never retract a key absent from the restored source).
    [Test]
    public async Task View_retries_heal_when_rebuild_fails_then_converges()
    {
        const string logical = "idswap-retry-src";
        const string shadow = "idswap-retry-restored";
        const string view = "idswap-retry-view";

        var faulting = new FaultInjectingProjection(new PredicateLatticeViewProjection(AdultFilter()));
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var latticeView = CreateAdultView(logical, view, faulting);

        await source.SetAsync("a", Person(30, "orig"));
        await source.SetAsync("b", Person(40, "orig"));
        await DrainToZeroAsync(view);
        Assert.That(await latticeView.CountAsync(), Is.EqualTo(2), "View must materialise the original source.");

        // Restored physical identity: 'a' changed, 'b' absent. Only a rebuild scan
        // can retract 'b'; the WAL tail alone never observes a delete for a key the
        // restored source simply never had.
        var shadowTree = _fixture.Cluster.Client.GetGrain<ILattice>(shadow);
        await shadowTree.SetAsync("a", Person(31, "restored"));
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(logical, shadow);

        // Arm one rebuild failure: the next drain detects the swap, starts the
        // rebuild, and the first projected key throws.
        faulting.ArmFailures(1);
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await maintainer.DrainAsync(),
            "The injected rebuild failure must surface from the heal drain.");

        // The failed rebuild must not have latched the new binding: the view still
        // shows the pre-swap projection (stale 'b' present) because nothing healed.
        Assert.That(await latticeView.GetAsync("b"), Is.EqualTo(Person(40, "orig")),
            "A failed rebuild must leave the view on its pre-swap state, not a half-healed one.");

        // The next drain (fault cleared) must re-detect the swap and rebuild
        // successfully, converging the view onto the restored source. Poll past the
        // read-handle cache TTL so the just-swapped active generation is observed
        // rather than a cached handle to the pre-swap generation.
        await DrainToZeroAsync(view);
        await WaitUntilAsync(async () => await latticeView.CountAsync() == 1,
            "View did not converge onto the restored source after the retried rebuild.");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await latticeView.GetAsync("a"), Is.EqualTo(Person(31, "restored")),
                "View must reflect the restored physical source once the retried rebuild succeeds.");
            Assert.That(await latticeView.GetAsync("b"), Is.Null,
                "The retried rebuild must retract the key absent from the restored source.");
            Assert.That(await latticeView.CountAsync(), Is.EqualTo(1));
        });
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string failureMessage)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail(failureMessage);
    }
}
