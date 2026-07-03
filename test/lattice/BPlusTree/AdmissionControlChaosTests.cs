using System.Text;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage proving the best-effort per-tree admission cap holds under
/// concurrent cross-shard write pressure: the enforcing cap eventually rejects
/// writes and keeps rejecting once the coalesced aggregate settles (no
/// under-count lets unbounded growth slip through), while an advisory-only tree
/// never rejects even under the same storm. Overshoot past the cap is expected
/// and allowed - the invariant is that the cap bites and stays bitten, not that
/// it bites at exactly the configured value.
/// </summary>
[TestFixture]
[Category("Chaos")]
public class AdmissionControlChaosTests
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
    public async Task Concurrent_cross_shard_writes_enforce_the_cap_and_keep_it_bitten()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.EnforcingTreeId);

        var rejections = 0;
        var successes = 0;

        // 8 concurrent writers, each writing distinct keys that hash across the
        // 4 shards, driven in waves so the aggregate has a chance to catch up.
        for (var wave = 0; wave < 12; wave++)
        {
            var tasks = new List<Task>();
            for (var writer = 0; writer < 8; writer++)
            {
                var key = $"c{wave}-{writer}";
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await tree.SetAsync(key, SmallValue());
                        Interlocked.Increment(ref successes);
                    }
                    catch (LatticeQuotaExceededException)
                    {
                        Interlocked.Increment(ref rejections);
                    }
                }));
            }

            await Task.WhenAll(tasks);
            await Task.Delay(150);
        }

        Assert.That(rejections, Is.GreaterThan(0),
            "the enforcing cap must reject at least some writes under concurrent cross-shard pressure");
        Assert.That(successes, Is.GreaterThanOrEqualTo((int)AdmissionControlClusterFixture.MaxLiveKeys),
            "writes up to the cap must have been admitted");

        // Once the storm subsides and the aggregate settles above the cap, a
        // fresh write must be rejected - proving there is no under-count that
        // would let the tree grow without bound.
        await Task.Delay(500);
        Assert.That(
            async () => await tree.SetAsync("post-storm", SmallValue()),
            Throws.InstanceOf<LatticeQuotaExceededException>(),
            "after the aggregate settles above the cap, admission must stay closed");
    }

    [Test]
    public async Task Advisory_only_tree_never_rejects_under_concurrent_pressure()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.AdvisoryTreeId);

        var rejections = 0;

        for (var wave = 0; wave < 8; wave++)
        {
            var tasks = new List<Task>();
            for (var writer = 0; writer < 8; writer++)
            {
                var key = $"adv-{wave}-{writer}";
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await tree.SetAsync(key, SmallValue());
                    }
                    catch (LatticeQuotaExceededException)
                    {
                        Interlocked.Increment(ref rejections);
                    }
                }));
            }

            await Task.WhenAll(tasks);
            await Task.Delay(100);
        }

        Assert.That(rejections, Is.EqualTo(0),
            "an advisory-only ceiling must never reject a write, even under concurrent pressure");
    }
}
