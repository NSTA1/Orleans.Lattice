using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of <see cref="ILattice.GetLeafProjectionDigestAsync"/> under
/// concurrent load. Validates two determinism invariants that gate the
/// digest's value as a cross-silo divergence detector:
///
/// <list type="bullet">
/// <item><description>Repeated-call stability — within a write-quiescent window
/// (after the chaos workload completes), every shard returns byte-identical
/// digests across successive calls. Non-determinism here would invalidate the
/// digest as a divergence-detection primitive.</description></item>
/// <item><description>Total-entry consistency — the sum of <see cref="LeafProjectionDigest.EntryCount"/>
/// across all shards equals <see cref="ILattice.CountAsync"/>, so the digest's
/// per-shard counts are accountable against the tree's own population view.</description></item>
/// </list>
///
/// The test sustains a concurrent writer + scanner + digest-poller mix to exercise
/// digest computation while the leaf grain handles foreground traffic, then
/// quiesces and asserts both invariants. Cross-silo replay equality (the
/// eventual destination of this fixture) becomes meaningful only once the
/// WAL-as-truth commit-path flip lands; the V1 deliverable verifies the digest
/// function itself is deterministic under load.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosDigestIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    private const int UniverseSize = 200;
    private const int WriterCount = 4;
    private const int ScannerCount = 2;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(8);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"chaos-digest-{i:D5}";

    [Test]
    public async Task Digest_is_stable_and_count_consistent_under_concurrent_load()
    {
        var tree = await _fixture.CreateTreeAsync($"chaos-digest-{Guid.NewGuid():N}");

        // Seed a fixed-size universe so CountAsync has a well-defined target.
        for (var i = 0; i < UniverseSize; i++)
        {
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"seed-{i}"));
        }

        using var cts = new CancellationTokenSource(ChaosDuration);
        var exceptions = new ConcurrentBag<Exception>();

        async Task WriterAsync(int writerId)
        {
            try
            {
                var seq = 0;
                var rng = new Random(writerId * 31 + 17);
                while (!cts.IsCancellationRequested)
                {
                    var idx = rng.Next(UniverseSize);
                    var v = $"v-{idx}-{writerId}-{seq++}";
                    try
                    {
                        await tree.SetAsync(KeyOf(idx), Encoding.UTF8.GetBytes(v), cts.Token);
                    }
                    catch (OperationCanceledException) { break; }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { exceptions.Add(ex); }
        }

        async Task ScannerAsync()
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        await foreach (var _ in tree.KeysAsync(cancellationToken: cts.Token))
                        {
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Orleans.Runtime.EnumerationAbortedException) { /* tolerated */ }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { exceptions.Add(ex); }
        }

        async Task DigestPollerAsync()
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
                        {
                            _ = await tree.GetLeafProjectionDigestAsync(s, cts.Token);
                        }
                    }
                    catch (OperationCanceledException) { break; }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { exceptions.Add(ex); }
        }

        var workers = new List<Task>();
        for (var w = 0; w < WriterCount; w++) workers.Add(WriterAsync(w));
        for (var s = 0; s < ScannerCount; s++) workers.Add(ScannerAsync());
        workers.Add(DigestPollerAsync());

        await Task.WhenAll(workers);

        Assert.That(exceptions, Is.Empty, "Chaos workers must not throw unhandled exceptions.");

        // Quiesce: sample digests twice with no intervening writes. Identical
        // bytes across calls is the determinism invariant.
        var firstPass = new LeafProjectionDigest[FourShardClusterFixture.TestShardCount];
        var secondPass = new LeafProjectionDigest[FourShardClusterFixture.TestShardCount];
        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
            firstPass[s] = await tree.GetLeafProjectionDigestAsync(s);
        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
            secondPass[s] = await tree.GetLeafProjectionDigestAsync(s);

        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            Assert.That(secondPass[s].Hash, Is.EqualTo(firstPass[s].Hash),
                $"Shard {s}: digest must be byte-stable across repeated calls when no writes occur in between.");
            Assert.That(secondPass[s].EntryCount, Is.EqualTo(firstPass[s].EntryCount),
                $"Shard {s}: entry count must be stable across repeated calls.");
        }

        // Total entry count across shards equals live key count. With a
        // fixed universe and no deletes during chaos, every key persists
        // and the live-count invariant holds exactly.
        var totalEntries = firstPass.Sum(d => (long)d.EntryCount);
        var liveCount = await tree.CountAsync();
        Assert.That(totalEntries, Is.EqualTo(liveCount),
            "Sum of per-shard digest EntryCount must equal CountAsync (no deletes in chaos workload).");
        Assert.That(liveCount, Is.EqualTo(UniverseSize),
            "Universe size must be preserved (writers only update existing keys).");
    }
}
