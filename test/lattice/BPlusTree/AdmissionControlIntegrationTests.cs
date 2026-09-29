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
    public async Task Enforcing_cap_rejects_a_conditional_batch_write_once_the_cap_is_reached()
    {
        // Regression: SetManyWherePredicateAsync skipped the admission check that
        // SetManyAsync performs, so a tree at its cap still accepted conditional
        // batches. Fill the tree through the unconditional path until the cap
        // bites, then the conditional write must be refused too.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.ConditionalEnforcingTreeId);
        var matching = Encoding.UTF8.GetBytes("{\"Score\":1}");

        var capReached = false;
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < 500 && stopwatch.Elapsed < TimeSpan.FromSeconds(30); i++)
        {
            try
            {
                await tree.SetAsync($"k{i}", matching);
            }
            catch (LatticeQuotaExceededException)
            {
                capReached = true;
                break;
            }
            await Task.Delay(25);
        }
        Assert.That(capReached, Is.True, "precondition: the unconditional path must reach the cap");

        // k0 holds a value the predicate admits, so only the cap can refuse this.
        // Polled because a fresh stateless-worker activation fails open until its
        // own first aggregate sample lands.
        var predicate = LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= 0, JsonLatticeSerializer<Scored>.Default);
        var entries = new List<KeyValuePair<string, byte[]>> { new("k0", Encoding.UTF8.GetBytes("{\"Score\":2}")) };
        LatticeQuotaExceededException? rejection = null;
        stopwatch.Restart();
        while (rejection is null && stopwatch.Elapsed < TimeSpan.FromSeconds(30))
        {
            try
            {
                await tree.SetManyWherePredicateAsync(entries, predicate);
            }
            catch (LatticeQuotaExceededException ex)
            {
                rejection = ex;
                break;
            }
            await Task.Delay(25);
        }

        Assert.That(rejection, Is.Not.Null,
            "a conditional batch write must be refused by an enforcing MaxLiveKeys cap");
        Assert.Multiple(() =>
        {
            Assert.That(rejection!.Dimension, Is.EqualTo(LatticeQuotaExceededException.KeysDimension));
            Assert.That(rejection.TreeId, Is.EqualTo(AdmissionControlClusterFixture.ConditionalEnforcingTreeId));
        });
    }

    private sealed record Scored(int Score);

    [Test]
    public async Task Enforcing_cap_rejects_every_atomic_batch_write_once_the_cap_is_reached()
    {
        // Regression: the single-tree atomic batch writes never checked the
        // per-tree admission caps. Their saga applies each leg under the prepared
        // scope, which bypasses admission by design, and the public entry points
        // skipped the check SetManyAsync performs - so a tree at its cap still
        // accepted every atomic batch.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.AtomicEnforcingTreeId);
        var matching = Encoding.UTF8.GetBytes("{\"Score\":1}");
        Assert.That(await FillUntilCapAsync(tree, matching), Is.True,
            "precondition: the unconditional path must reach the cap");

        // k0 holds a value the predicate admits, so only the cap can refuse the
        // guarded variants.
        var predicate = LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= 0, JsonLatticeSerializer<Scored>.Default);
        var writes = new (string Name, Func<int, Task> Write)[]
        {
            ("SetManyAtomicAsync(entries)",
                i => tree.SetManyAtomicAsync(Batch($"a{i}"))),
            ("SetManyAtomicAsync(entries, operationId)",
                i => tree.SetManyAtomicAsync(Batch($"b{i}"), $"op-b{i}")),
            ("SetManyAtomicAsync(upserts, deletes, operationId)",
                i => tree.SetManyAtomicAsync(Batch($"c{i}"), Array.Empty<string>(), $"op-c{i}")),
            ("SetManyAtomicWhereAsync(entries, predicate)",
                _ => tree.SetManyAtomicWhereAsync(Batch("k0"), predicate)),
            ("SetManyAtomicWhereAsync(entries, predicate, operationId)",
                i => tree.SetManyAtomicWhereAsync(Batch("k0"), predicate, $"op-e{i}")),
        };

        var admitted = new List<string>();
        foreach (var (name, write) in writes)
        {
            // Polled because a fresh stateless-worker activation fails open until
            // its own first aggregate sample lands.
            var rejection = await PollForQuotaRejectionAsync(write);
            if (rejection is null)
            {
                admitted.Add(name);
                continue;
            }

            Assert.Multiple(() =>
            {
                Assert.That(rejection.Dimension, Is.EqualTo(LatticeQuotaExceededException.KeysDimension), name);
                Assert.That(rejection.TreeId, Is.EqualTo(AdmissionControlClusterFixture.AtomicEnforcingTreeId), name);
            });
        }

        Assert.That(admitted, Is.Empty,
            "every atomic batch write must be refused by an enforcing MaxLiveKeys cap");
    }

    [Test]
    public async Task Enforcing_cap_still_admits_a_delete_only_atomic_batch()
    {
        // A delete-only atomic batch can only shrink the tree, so the cap must
        // never stop a caller from getting back under it.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(AdmissionControlClusterFixture.AtomicDeleteEnforcingTreeId);
        Assert.That(await FillUntilCapAsync(tree, SmallValue()), Is.True,
            "precondition: the unconditional path must reach the cap");

        Assert.DoesNotThrowAsync(async () => await tree.SetManyAtomicAsync(
            new List<KeyValuePair<string, byte[]>>(), new[] { "k0" }, "op-delete-only"));
        Assert.That(await tree.GetAsync("k0"), Is.Null);
    }

    private static List<KeyValuePair<string, byte[]>> Batch(string key) =>
        new() { new(key, Encoding.UTF8.GetBytes("{\"Score\":2}")) };

    private static async Task<bool> FillUntilCapAsync(ILattice tree, byte[] value)
    {
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < 500 && stopwatch.Elapsed < TimeSpan.FromSeconds(30); i++)
        {
            try
            {
                await tree.SetAsync($"k{i}", value);
            }
            catch (LatticeQuotaExceededException)
            {
                return true;
            }
            await Task.Delay(25);
        }
        return false;
    }

    private static async Task<LatticeQuotaExceededException?> PollForQuotaRejectionAsync(Func<int, Task> write)
    {
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; stopwatch.Elapsed < TimeSpan.FromSeconds(30); i++)
        {
            try
            {
                await write(i);
            }
            catch (LatticeQuotaExceededException ex)
            {
                return ex;
            }
            await Task.Delay(25);
        }
        return null;
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
