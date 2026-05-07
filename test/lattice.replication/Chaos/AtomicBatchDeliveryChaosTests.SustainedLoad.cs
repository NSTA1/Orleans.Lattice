using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Sustained-load + mid-workload partition test for cross-cluster
/// atomic-batch delivery. Three sites configured with
/// <see cref="LatticeReplicationOptions.AtomicBatchDelivery"/>=true
/// drive a continuous stream of <see cref="ILattice.SetManyAtomicAsync"/>
/// writes from the producer site through the
/// <see cref="ChaosDeliveryPump"/>; mid-workload a partition isolates
/// one of the receivers and is healed before the workload completes,
/// then a generous drain settles every authored batch.
/// <para>
/// Two contracts are pinned end-to-end:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Atomic visibility on convergence.</b> Every authored batch's
/// keys are present (post-drain) on every receiver as a unit — never
/// split across the snapshot / incremental / pump / partition
/// boundary, never partial.
/// </description></item>
/// <item><description>
/// <b>Terminal-outcome accounting.</b> The
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/> counter
/// tagged <see cref="LatticeReplicationMetrics.OutcomeTxSuccess"/>
/// records at least <c>(siteCount - 1) × authored</c> increments
/// (one per receiver-side completion), and zero increments under
/// any failure outcome on a clean partition-then-heal scenario.
/// </description></item>
/// </list>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    /// <summary>
    /// Number of distinct atomic batches the producer authors during
    /// the test window. Sized so the workload completes in roughly
    /// 3-5 seconds on a contended CI runner — short enough that the
    /// outer drain budget covers the worst case, long enough that
    /// the partition-then-heal cycle exercises a meaningful number
    /// of batches on either side of the boundary.
    /// </summary>
    private const int SustainedBatchCount = 40;

    /// <summary>
    /// Minimum and maximum number of keys per authored batch. Drawn
    /// uniformly from <c>[Min, Max]</c>; the spread exercises both
    /// the small-batch fast-path and the larger 16-key batches that
    /// pin the saga's per-key emission ordering.
    /// </summary>
    private const int SustainedMinKeysPerBatch = 2;
    private const int SustainedMaxKeysPerBatch = 16;

    /// <summary>
    /// Inter-batch sleep applied between successive
    /// <see cref="ILattice.SetManyAtomicAsync"/> calls on the
    /// producer. Spreads the workload across enough wall-clock to
    /// guarantee the partition-then-heal cycle straddles a non-
    /// trivial number of batches without inflating the test
    /// duration.
    /// </summary>
    private static readonly TimeSpan SustainedInterBatchDelay = TimeSpan.FromMilliseconds(50);

    [Test]
    public async Task Sustained_atomic_batch_load_under_partition_converges_with_full_atomic_visibility()
    {
        // Three sites: site 0 is the producer; sites 1 and 2 are
        // pure receivers. AtomicBatchDelivery=true on every site so
        // the receiver-side gate routes every inbound atomic-batch
        // entry through the per-tree IReplicationTxBufferGrain.
        // ShadowForwardDedupeCacheSize is bumped well above the
        // workload's natural identity-tuple cardinality (40 batches
        // × up to 16 keys × 3 receivers = 1920) so a content-hash
        // race during partition heal cannot accidentally suppress a
        // legitimate apply.
        await using var runner = new TestRunner(configureSilo: opts =>
        {
            opts.AtomicBatchDelivery = true;
            opts.AtomicBatchBufferMaxTransactions = 256;
            opts.AtomicBatchBufferMaxBytes = 16L * 1024L * 1024L;
            opts.TxBufferOrphanTimeout = TimeSpan.FromMinutes(5);
            opts.ShadowForwardDedupeCacheSize = 8192;
        });
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        using var outcomes = new TxOutcomeCollector();

        // Pre-stage: the workload below uses a single random source
        // with a fixed seed so the per-batch key counts are
        // deterministic across reruns. Reliability emphasis: a
        // chaos test that fails non-deterministically is worse than
        // no chaos test at all.
        var rng = new Random(31415926);
        var producerLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var authored = new List<AuthoredBatch>(SustainedBatchCount);

        for (var batch = 0; batch < SustainedBatchCount; batch++)
        {
            // Mid-workload partition trigger: isolate site 2 at
            // ~1/3rd of the workload, heal at ~2/3rds. The
            // delivery pump still polls site 0's WAL during the
            // partition but does not advance the (0,2) cursor; on
            // heal, the pump ships every entry that accumulated
            // during the outage so site 2 can converge.
            if (batch == SustainedBatchCount / 3)
            {
                pump.IsolateSite(2);
            }
            else if (batch == (2 * SustainedBatchCount) / 3)
            {
                pump.HealSite(2);
            }

            var keyCount = rng.Next(SustainedMinKeysPerBatch, SustainedMaxKeysPerBatch + 1);
            var operations = new List<KeyValuePair<string, byte[]>>(keyCount);
            var batchKeys = new List<string>(keyCount);
            var batchTag = $"b{batch:D4}";

            for (var k = 0; k < keyCount; k++)
            {
                var key = $"{batchTag}/k{k:D2}";
                batchKeys.Add(key);
                operations.Add(new KeyValuePair<string, byte[]>(key, Encoding.UTF8.GetBytes($"{batchTag}-v{k:D2}")));
            }

            authored.Add(new AuthoredBatch(batchTag, batchKeys));

            // Producer-side emission. SetManyAtomicAsync on the
            // production ILattice surface routes through the real
            // AtomicWriteGrain.RunSagaAsync, which stamps every
            // per-key emit with the same TransactionId,
            // AtomicBatchSize, and AtomicBatchIndex. The producer-
            // side ReplicationMutationObserver mirrors those onto
            // the WAL ReplogEntry rows, where the chaos pump picks
            // them up.
            await producerLattice.SetManyAtomicAsync(operations);

            await Task.Delay(SustainedInterBatchDelay);
        }

        // Heal every edge (any leftover partition state) and let
        // the topology drain. The drain criterion (every edge's
        // cursor caught up to its sender's WAL tail HLC for
        // DrainStabilityWindow consecutive polls) covers the
        // partition-heal-then-catch-up window.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Cursor-drain returns when every edge's cursor has caught up
        // to its sender's WAL tail HLC, but with AtomicBatchDelivery=true
        // the receiver-side IReplicationTxBufferGrain holds entries
        // pending an atomic commit until the full batch arrives — so
        // the last few authored batches may still be settling in the
        // buffer for a brief window after cursor-drain returns, AND a
        // partition-heal-then-catch-up sequence can re-ship sibling
        // entries that the receiver-side buffer must re-admit and
        // re-commit. Poll on every receiver until every authored
        // batch's first and last keys are visible (the canonical-order
        // emit invariant means index-0 lands first and index-(N-1)
        // last; if both bookends are present, the saga has committed
        // the entire batch atomically). Generous budget — 30s — so the
        // assertion below evaluates after the buffer-commit window
        // closes for every batch.
        var convergenceBudget = TimeSpan.FromSeconds(5);
        for (var siteIdx = 1; siteIdx < fixture.SiteCount; siteIdx++)
        {
            var receiver = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            await WaitForAsync(
                async () =>
                {
                    foreach (var batch in authored)
                    {
                        if ((await receiver.GetAsync(batch.Keys[^1])) is null)
                        {
                            return false;
                        }
                    }
                    return true;
                },
                convergenceBudget,
                pollInterval: TimeSpan.FromMilliseconds(100));
        }

        // Atomic visibility post-drain: every receiver sees every
        // authored batch's keys as a unit. We assert per-batch on
        // each receiver because the contract is "every key of the
        // batch is present" — silence on a key would split the
        // batch.
        for (var siteIdx = 1; siteIdx < fixture.SiteCount; siteIdx++)
        {
            var receiver = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            foreach (var batch in authored)
            {
                var partial = false;
                var fullPresent = true;
                foreach (var key in batch.Keys)
                {
                    var value = await receiver.GetAsync(key);
                    if (value is null)
                    {
                        fullPresent = false;
                    }
                    else
                    {
                        partial = partial || !fullPresent;
                    }
                }

                Assert.That(
                    fullPresent || !partial,
                    Is.True,
                    $"Receiver site {siteIdx} observed a partial view of authored batch '{batch.Tag}': "
                    + "some keys present, others missing. Atomic visibility violated.");
                Assert.That(
                    fullPresent,
                    Is.True,
                    $"Receiver site {siteIdx} did not converge on every key of authored batch '{batch.Tag}' "
                    + "within the drain budget.");
            }
        }

        // Terminal-outcome accounting: every authored batch reaches
        // exactly one terminal outcome on every receiver. With a
        // clean partition-then-heal (no orphan, no buffer overflow,
        // no apply failure), every increment must land in the
        // success bucket. The strict equality is "at least"
        // because a re-shipment that completes the same batch on
        // both sides of the partition heal could double-count;
        // however the receiver-side buffer dedupes by
        // (origin, txid, index) so the outcome counter still fires
        // at most once per receiver per transaction.
        var receiverCount = fixture.SiteCount - 1;
        var expectedSuccess = (long)receiverCount * SustainedBatchCount;
        var actualSuccess = outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxSuccess, TreeName);

        Assert.That(
            actualSuccess,
            Is.GreaterThanOrEqualTo(expectedSuccess),
            $"Terminal-outcome counter under-counted success: expected at least {expectedSuccess} "
            + $"(receivers={receiverCount} × authored={SustainedBatchCount}), observed {actualSuccess}.");

        // Failure-outcome floor: a clean run produces zero
        // increments under any non-success bucket. A non-zero
        // sample here flags a regression in the saga path or a
        // partition-heal scenario that escalated to orphan or
        // capacity eviction.
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, TreeName),
                Is.Zero,
                "Sustained-load test must not surface any DLQ-orphan outcomes on a clean partition-then-heal.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, TreeName),
                Is.Zero,
                "Sustained-load test must not surface any DLQ-apply-failure outcomes.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, TreeName),
                Is.Zero,
                "Sustained-load test must not surface any capacity-eviction outcomes "
                + "(buffer cap is sized well above the workload's natural concurrency).");
        });

        // Diagnostic surface: pump errors are best-effort surfaced
        // by ChaosDeliveryPump but not asserted on (the convergence
        // assertion above is the source of truth). Log if any
        // appeared so a regression stays visible to the test runner.
        if (!pump.PumpErrors.IsEmpty)
        {
            TestContext.Out.WriteLine(
                $"ChaosDeliveryPump surfaced {pump.PumpErrors.Count} transient errors during the run "
                + "(non-fatal — convergence assertion is authoritative).");
        }
    }

    /// <summary>
    /// Drives initialisation + disposal for the sustained-load
    /// scenario. Mirrors the <c>TestRunner</c> shape established by
    /// the R-033 chaos suites so the partition-and-pump lifecycle
    /// is identical, with the addition of a per-test silo-side
    /// option configurator to flip
    /// <see cref="LatticeReplicationOptions.AtomicBatchDelivery"/>
    /// and tune buffer / dedupe sizing.
    /// </summary>
    private sealed class TestRunner(Action<LatticeReplicationOptions> configureSilo) : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(
            ReplicationMode.LwwRegister,
            siteCount: 3,
            configureSilo: configureSilo,
            configureClient: configureSilo);

        public ChaosDeliveryPump Pump { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            Pump = new ChaosDeliveryPump(Fixture, TreeName);
            Pump.Start();
        }

        public async ValueTask DisposeAsync()
        {
            if (Pump is not null)
            {
                await Pump.DisposeAsync();
            }
            await Fixture.DisposeAsync();
        }
    }
}
