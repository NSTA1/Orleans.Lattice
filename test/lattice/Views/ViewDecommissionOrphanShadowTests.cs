using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression for the shadow generation an interrupted rebuild leaves behind.
/// A derive-locally rebuild writes the next generation tree
/// (<c>ActiveGeneration + 1</c>) before its durable swap, so an interrupted build
/// (a projection fault, an operator cancel, a silo crash) leaves that partial
/// tree with no durable record. Decommission deleted only generations
/// <c>0..ActiveGeneration</c> (and any pending reclaim), so the deleted view's
/// partially rebuilt rows survived its deletion indefinitely, contradicting the
/// "a deleted view leaves no orphaned tree" teardown contract.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewDecommissionOrphanShadowTests
{
    private const string Provider = "tests.orphan-shadow.v1";
    private const string ViewName = "orphan-shadow-view";
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private static volatile string? s_failOnKey;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private ILatticeViewFactory Factory =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<ILatticeViewFactory>();

    [Test]
    public async Task Decommission_deletes_the_shadow_generation_an_interrupted_rebuild_left_behind()
    {
        var source = _cluster.Client.GetGrain<ILattice>("orphan-src");
        await source.SetAsync("a", [1]);
        await source.SetAsync("b", [2]);

        var view = await Factory.CreateAsync(
            source, ViewName, new LatticeRuntimeViewProjectionDescriptor(Provider, []));
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("a"), Is.EqualTo(new byte[] { 1 }));

        // Interrupt a shadow rebuild after it has written "a" into the next
        // generation tree but before the swap.
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(ViewName);
        var activeTreeId = await maintainer.GetActiveTreeIdAsync();
        Assert.That(activeTreeId, Is.EqualTo(LatticeViewTrees.ComposeTreeId(ViewName)), "The test assumes the view still serves generation 0.");
        var shadowTreeId = LatticeViewTrees.ComposeTreeId(ViewName, 1);
        s_failOnKey = "b";
        try
        {
            Assert.That(async () => await maintainer.RebuildAsync(), Throws.Exception);
        }
        finally
        {
            s_failOnKey = null;
        }

        Assert.That(await view.GetAsync("a"), Is.EqualTo(new byte[] { 1 }), "The interrupted rebuild must not have swapped.");
        Assert.That(await ReadShadowAsync(shadowTreeId, "a"), Is.EqualTo(new byte[] { 1 }), "The interrupted rebuild must have left a partial shadow generation.");

        await Factory.DeleteAsync(ViewName);

        Assert.That(
            await ReadShadowAsync(shadowTreeId, "a"),
            Is.Null,
            "Decommission must delete the partial shadow generation, not leave the deleted view's rows behind.");
    }

    private async Task<byte[]?> ReadShadowAsync(string treeId, string key)
    {
        using var scope = ViewReadContext.BeginScope();
        try
        {
            return await _cluster.Client.GetGrain<ILattice>(treeId).GetAsync(key);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("deleted", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views => views.AddRuntimeProjectionProvider(
                Provider,
                (_, context) => new LatticeViewDefinition(context.ViewName, new FaultingIdentityProjection())));
        }
    }

    private sealed class FaultingIdentityProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            if (mutation.Key is not null && string.Equals(mutation.Key, s_failOnKey, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Injected projection fault.");
            }

            return mutation.Kind switch
            {
                MutationKind.Set => [ViewWrite.Upsert(mutation.Key!, mutation.Value!, mutation.Timestamp)],
                MutationKind.Delete => [ViewWrite.Delete(mutation.Key!, mutation.Timestamp)],
                _ => [],
            };
        }
    }
}
