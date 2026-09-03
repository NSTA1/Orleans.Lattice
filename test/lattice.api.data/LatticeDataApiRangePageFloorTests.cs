using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Regression coverage for the range-read page-size clamp
/// (<c>LatticeDataApi.ClampPageSize</c>). The sibling range-<b>delete</b> path
/// floors its step size at one (<c>Math.Max(1, RangeDeleteStepSize)</c>), but the
/// read path historically only capped the size from above
/// (<c>Math.Min(size, MaxRangePageSize)</c>) with no matching floor. A
/// non-positive <see cref="LatticeApiDataOptions.MaxRangePageSize"/> - which
/// nothing validates at registration - therefore collapsed every resolved page
/// size to zero, and the cursor grain rejects a zero page with
/// <see cref="ArgumentOutOfRangeException"/>, so a single misconfigured option
/// faulted every bounded range read rather than returning a bounded page.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeDataApiRangePageFloorTests
{
    private TestCluster _cluster = null!;

    private ILatticeDataApi Api =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services
            .GetRequiredService<ILatticeDataApi>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task ReadRangeAsync_floors_a_non_positive_max_page_size_to_one_instead_of_faulting()
    {
        const string tree = "range-floor";
        await RegisterTreeAsync(tree);

        await Api.SetAsync(tree, "a", new byte[] { 1 });
        await Api.SetAsync(tree, "b", new byte[] { 2 });
        await Api.SetAsync(tree, "c", new byte[] { 3 });

        // MaxRangePageSize is misconfigured to 0, so the pre-fix clamp resolved
        // every page size to 0 and the cursor grain faulted the read. With the
        // floor, the read succeeds and yields a single-entry page that still
        // paginates the rest of the range.
        var page = await Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(1), "the resolved page size is floored to one, not zero");
            Assert.That(page.ContinuationToken, Is.Not.Null.And.Not.Empty, "the range is not drained, so a continuation is returned");
        });
    }

    private async Task RegisterTreeAsync(string treeId)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
            WalPartitions = 1,
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeDataApi(o => o.MaxRangePageSize = 0);
        }
    }
}
