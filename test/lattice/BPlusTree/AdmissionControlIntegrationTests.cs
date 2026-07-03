using System.Diagnostics;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the opt-in, fail-open per-tree admission control.
/// A tree with an enforcing <see cref="LatticeOptions.MaxLiveKeys"/> cap
/// eventually rejects writes past the cap with
/// <see cref="LatticeQuotaExceededException"/> (best-effort: the coalesced
/// cross-shard aggregate may overshoot slightly before it bites), while a tree
/// configured with only an advisory ceiling never rejects a write.
/// </summary>
[TestFixture]
[Category("Integration")]
public class AdmissionControlIntegrationTests
{
    private AdmissionControlClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AdmissionControlClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] SmallValue() => Encoding.UTF8.GetBytes("v");

    [Test]
    public async Task Enforcing_cap_eventually_rejects_writes_past_the_live_key_cap()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.EnforcingTreeId);

        LatticeQuotaExceededException? rejection = null;
        var stopwatch = Stopwatch.StartNew();
        // Keep writing fresh keys; the coalesced aggregate refreshes
        // asynchronously, so the cap bites best-effort. Bounded by a generous
        // timeout so a hung propagation fails loudly rather than hanging.
        for (var i = 0; i < 500 && stopwatch.Elapsed < TimeSpan.FromSeconds(30); i++)
        {
            try
            {
                await tree.SetAsync($"k{i}", SmallValue());
            }
            catch (LatticeQuotaExceededException ex)
            {
                rejection = ex;
                break;
            }
            await Task.Delay(25);
        }

        Assert.That(rejection, Is.Not.Null,
            "an enforcing MaxLiveKeys cap must eventually reject a write once the aggregate catches up");
        Assert.Multiple(() =>
        {
            Assert.That(rejection!.Dimension, Is.EqualTo(LatticeQuotaExceededException.KeysDimension));
            Assert.That(rejection.Limit, Is.EqualTo(AdmissionControlClusterFixture.MaxLiveKeys));
            Assert.That(rejection.Current, Is.GreaterThanOrEqualTo(AdmissionControlClusterFixture.MaxLiveKeys));
            Assert.That(rejection.TreeId, Is.EqualTo(AdmissionControlClusterFixture.EnforcingTreeId));
        });
    }

    [Test]
    public async Task Advisory_only_tree_never_rejects_a_write()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.AdvisoryTreeId);

        // Write well past the advisory ceiling; none of these must be rejected.
        for (var i = 0; i < 40; i++)
        {
            await tree.SetAsync($"a{i}", SmallValue());
            await Task.Delay(10);
        }

        // A final write after the aggregate has had time to catch up must still
        // succeed: an advisory ceiling is dry-run only.
        Assert.DoesNotThrowAsync(async () => await tree.SetAsync("final", SmallValue()));

        var read = await tree.GetAsync("final");
        Assert.That(read, Is.EqualTo(SmallValue()));
    }
}
