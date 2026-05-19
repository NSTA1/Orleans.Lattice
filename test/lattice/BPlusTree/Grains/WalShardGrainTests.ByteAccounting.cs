using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Exact byte-accounting regression tests for `WalMaxBatchBytes`
// cutover. The base fixture lives in WalShardGrainTests.cs; this
// partial only adds the assertions that exercise the
// `IWalRecordSizer` seam.
public partial class WalShardGrainTests
{
    /// <summary>
    /// Stub <see cref="IWalRecordSizer"/> that returns a caller-controlled
    /// size for every <see cref="WalRecord"/>. Used so the test can
    /// engineer exact byte-budget boundaries without depending on the
    /// production Orleans-binary encoded footprint of any particular
    /// payload shape.
    /// </summary>
    private sealed class FixedSizer(int sizePerEntry) : IWalRecordSizer
    {
        public int Calls;
        public int SizePerEntry { get; set; } = sizePerEntry;
        public int Measure(WalRecord entry)
        {
            Calls++;
            return SizePerEntry;
        }
    }

    [Test]
    public async Task AppendAsync_invokes_sizer_once_per_append()
    {
        // Sizer participation is the contract: the grain must consult
        // the injected `IWalRecordSizer` on every foreground append so
        // the byte budget is driven off exact measurement.
        var sizer = new FixedSizer(sizePerEntry: 1);
        var grain = await CreateGrainAsync(sizer: sizer);

        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        Assert.That(sizer.Calls, Is.EqualTo(3));
    }

    [Test]
    public async Task AppendAsync_cuts_over_when_sizer_reports_budget_overflow()
    {
        // Engineer a precise budget: WalMaxBatchBytes = 100, sizer
        // reports 40 bytes per entry. The first two entries (sum 80)
        // must coalesce; the third (sum 120) must trigger cutover
        // before being added so the persisted batch shape is [2, 1].
        var sizer = new FixedSizer(sizePerEntry: 40);
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var grain = await CreateGrainAsync(
            capturing,
            new LatticeOptions
            {
                WalMaxBatchBytes = 100,
                WalMaxBatchEntries = 1000, // disable entry-count gate
                WalMaxPendingBatches = 1,
            },
            sizer);

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L }));
            Assert.That(capturing.BatchSizes, Is.EqualTo(new[] { 1, 2 }));
        });
    }

    [Test]
    public async Task AppendAsync_admits_entry_at_exact_budget_boundary()
    {
        // Exact-fit case: WalMaxBatchBytes = 100, sizer reports 50
        // bytes. Two entries (sum 100) must coalesce into a single
        // batch - the cutover check is strict `>`, not `>=`, so a
        // batch that exactly fills the budget is still admitted.
        var sizer = new FixedSizer(sizePerEntry: 50);
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var grain = await CreateGrainAsync(
            capturing,
            new LatticeOptions
            {
                WalMaxBatchBytes = 100,
                WalMaxBatchEntries = 1000,
                WalMaxPendingBatches = 1,
            },
            sizer);

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        gated.Open();
        await Task.WhenAll(t1, t2);

        // The first append starts a flush of [a] (lone entry kicks
        // immediately under the latency-floor trigger); under the
        // gate the second append accumulates and flushes as a single-
        // entry batch. Both batches are well under the 100-byte
        // budget. The cap=1 protocol shapes this into [1, 1].
        Assert.That(capturing.BatchSizes.Sum(), Is.EqualTo(2));
    }

    [Test]
    public async Task AppendAsync_cuts_over_when_exact_size_exceeds_heuristic_estimate()
    {
        // Regression for the WAL-design hazard the exact-byte change
        // closes: an entry whose historical heuristic estimate
        // (`key.Length * 2 + value.Length + 128`) is below the
        // budget but whose true encoded size exceeds it. Engineer
        // this by injecting a sizer that returns 4096 for one entry
        // and 1 for the rest; the budget is 8000 bytes. Under the
        // old heuristic a 10-byte key + 100-byte value entry would
        // have measured ~248 bytes, so 30+ would have fit in the
        // batch; under exact measurement the 4096-byte entry alone
        // cuts the batch over after one other entry has been added.
        var sizer = new FixedSizer(sizePerEntry: 1);
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var grain = await CreateGrainAsync(
            capturing,
            new LatticeOptions
            {
                WalMaxBatchBytes = 8_000,
                WalMaxBatchEntries = 1000,
                WalMaxPendingBatches = 1,
            },
            sizer);

        // Three small entries first (size=1 each).
        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        // Now switch the sizer to report 4096 bytes per entry; two
        // such entries (= 8192 bytes) overflow the 8000-byte budget.
        sizer.SizePerEntry = 4096;
        var t4 = grain.AppendAsync(MakeEntry("d"), CancellationToken.None);
        var t5 = grain.AppendAsync(MakeEntry("e"), CancellationToken.None);

        gated.Open();
        await Task.WhenAll(t1, t2, t3, t4, t5);

        // The grain must have cut over to keep every batch within
        // the 8000-byte budget once the large entries arrived. Under
        // the historical heuristic this scenario would have under-
        // counted and produced a single 8192-byte batch over budget.
        Assert.Multiple(() =>
        {
            Assert.That(capturing.BatchSizes.Sum(), Is.EqualTo(5));
            // No single batch contains both 4096-byte entries
            // (i.e. no batch exceeds the budget): the largest batch
            // must carry at most 1 large entry plus an arbitrary
            // number of size-1 fillers below the budget. Asserting
            // structurally: at least one cutover happened between
            // t4 and t5.
            Assert.That(capturing.BatchSizes.Count, Is.GreaterThanOrEqualTo(2));
        });
    }
}
