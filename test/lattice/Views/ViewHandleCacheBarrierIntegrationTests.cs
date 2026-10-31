using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// <see cref="ILatticeView.WaitForSourceHeadAsync"/> and
/// <see cref="ILatticeView.WaitForSourceHlcAsync"/> are read-your-writes barriers on
/// the same handle a caller then reads through (#5315). A barrier that drives a
/// rebind after a source identity change makes the maintainer swap to a new view
/// generation; the read handle caches the active generation's tree id for
/// <see cref="LatticeViewOptions.ReadHandleCacheTtl"/>, so unless the barrier drops
/// that cache, a read straight after it serves the retired generation.
/// <para>
/// The read-handle cache is held for an hour so the handle is deterministically
/// warm, and the view's drain timer is parked, so the only drains are those the
/// barrier drives.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewHandleCacheBarrierIntegrationTests
{
    private const string HeadViewName = "handle-cache-head-view";
    private const string HeadSourceTreeId = "handle-cache-head-src";
    private const string HlcViewName = "handle-cache-hlc-view";
    private const string HlcSourceTreeId = "handle-cache-hlc-src";

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

    /// <summary>
    /// Writes <c>old</c> under <c>k</c>, warms the view handle's generation cache by
    /// reading it, then moves the source's alias to a physical copy holding
    /// <c>new</c>, so the next barrier rebinds the view onto a new generation.
    /// </summary>
    private async Task<ILatticeView> WarmThenCutOverAsync(string viewName, string sourceTreeId)
    {
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        await source.SetAsync("k", Value("old"));

        var view = await SiloServices.GetRequiredService<ILatticeViewFactory>().GetAsync(viewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("k"), Is.EqualTo(Value("old")),
            "precondition: the handle has read, and so cached, the view's first generation");

        var copy = $"{sourceTreeId}-copy";
        await _cluster.Client.GetGrain<ILattice>(copy).SetAsync("k", Value("new"));
        await _cluster.GrainFactory.GetLatticeRegistry().SetAliasAsync(sourceTreeId, copy);
        return view;
    }

    [Test]
    public async Task Read_on_the_same_handle_after_WaitForSourceHeadAsync_sees_the_generation_a_cutover_rebind_swapped_in()
    {
        var view = await WarmThenCutOverAsync(HeadViewName, HeadSourceTreeId);

        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAsync("k"), Is.EqualTo(Value("new")),
            "a read on the barrier's own handle must observe the generation the barrier's rebind swapped in");
    }

    [Test]
    public async Task Read_on_the_same_handle_after_WaitForSourceHlcAsync_sees_the_generation_a_cutover_rebind_swapped_in()
    {
        var view = await WarmThenCutOverAsync(HlcViewName, HlcSourceTreeId);

        // The copy's write is the newest source write, so its HLC is the target.
        var target = await _cluster.Client.GetGrain<ILattice>($"{HlcSourceTreeId}-copy").GetWithVersionAsync("k");
        await view.WaitForSourceHlcAsync(target.Version, Barrier);

        Assert.That(await view.GetAsync("k"), Is.EqualTo(Value("new")),
            "a read on the barrier's own handle must observe the generation the barrier's drains swapped in");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views =>
            {
                views.AddView(HeadViewName, HeadSourceTreeId, new PredicateLatticeViewProjection());
                views.AddView(HlcViewName, HlcSourceTreeId, new PredicateLatticeViewProjection());
            });
            foreach (var viewName in new[] { HeadViewName, HlcViewName })
            {
                siloBuilder.ConfigureLatticeView(viewName, o =>
                {
                    o.CoalesceWindow = TimeSpan.FromHours(1);
                    o.ReadHandleCacheTtl = TimeSpan.FromHours(1);
                    o.OldGenerationReclaimGrace = TimeSpan.FromHours(2);
                });
            }
        }
    }
}
