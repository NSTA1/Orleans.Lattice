using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// <see cref="ILatticeView.WaitForSourceHeadAsync"/> is a read-your-writes barrier:
/// it returns only once every committed source write that existed at the call is
/// reflected in the view (#5308). A write that preserves a peer's HLC
/// (<see cref="ILattice.MergeAsync(string, CancellationToken)"/>) can sit below the
/// highest source HLC the view has already applied, so a barrier that compares HLC
/// watermarks returns before the merged write is folded.
/// <para>
/// The view's background drain timer is parked for the life of the fixture, so the
/// only drains are the ones the barrier itself drives: whether a write is visible
/// after the barrier depends on the barrier alone, never on timer scheduling.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewSourceHeadBarrierIntegrationTests
{
    private const string ViewName = "head-barrier-filter";
    private const string SourceTreeId = "head-barrier-src";
    private const string PeerTreeId = "head-barrier-peer";

    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
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

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static byte[] Value(string text) => Encoding.UTF8.GetBytes(text);

    [Test]
    public async Task WaitForSourceHeadAsync_reflects_a_merged_write_whose_peer_hlc_is_below_the_applied_watermark()
    {
        var source = _cluster.Client.GetGrain<ILattice>(SourceTreeId);
        var peer = _cluster.Client.GetGrain<ILattice>(PeerTreeId);

        // The peer write is stamped first, so its HLC is below every later source write.
        await peer.SetAsync("merged", Value("from-peer"));
        await source.SetAsync("local", Value("from-source"));

        var view = await SiloServices.GetRequiredService<ILatticeViewFactory>().GetAsync(ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("local"), Is.EqualTo(Value("from-source")),
            "precondition: the view has applied the source write, so its watermark is above the peer HLC");

        // The merge keeps the peer's HLC, which is below what the view has applied.
        // The merge coordinator's own pass is driven directly rather than through
        // its reminder, which does not tick inside the test cluster.
        await source.MergeAsync(PeerTreeId);
        await _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(SourceTreeId).RunMergePassAsync();
        Assert.That(await source.IsMergeCompleteAsync(), Is.True, "precondition: the merge pass completed");
        Assert.That(await source.GetAsync("merged"), Is.EqualTo(Value("from-peer")),
            "precondition: the merged write is committed at the source");

        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAsync("merged"), Is.EqualTo(Value("from-peer")),
            "a write committed at the source before the barrier must be visible after it, whatever its HLC");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views => views.AddView(ViewName, SourceTreeId, new PredicateLatticeViewProjection()));
            siloBuilder.ConfigureLatticeView(ViewName, o =>
            {
                o.CoalesceWindow = TimeSpan.FromHours(1);
                o.ReadHandleCacheTtl = TimeSpan.FromTicks(1);
            });
        }
    }
}
