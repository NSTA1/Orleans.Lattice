using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for issue #5831: a fresh partition's first stamped writes must not race
/// each other to create the same routing-generation row, and the first stamped
/// append under a generation the partition has not loaded must not cost the writer a
/// rejection and a separate claim.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task Concurrent_re_presentations_of_one_generation_create_its_row_once()
    {
        // Every write of a fresh tree that reaches a partition before its shard root's
        // generation is loaded re-presents the same (initiator, generation). They must
        // not each read the row absent and race to create it: the losers fail with a
        // 409, re-read and retry, adding storage writes to a saturated store.
        var storage = new SlowRowStorage(new EnumerableMemoryGrainStorage(), WalAppendFenceRows.GenerationGrainType);
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.ShardRoot(3);

        var claims = Enumerable.Range(0, 6)
            .Select(_ => grain.ClaimRoutingGenerationAsync(initiator, 1, CancellationToken.None))
            .ToArray();
        var results = await Task.WhenAll(claims);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(r => r.Accepted), Is.All.True);
            Assert.That(storage.Conflicts, Is.Zero, "concurrent re-presentations raced to create the generation row");
            Assert.That(storage.Writes, Is.EqualTo(1), "the generation row was written other than once");
        });
    }

    [Test]
    public async Task A_stamped_append_under_an_unloaded_generation_loads_it_and_is_admitted()
    {
        // The first stamped append a partition receives under a generation it has not
        // loaded loads that generation itself, rather than rejecting it and costing the
        // writer a separate claim call and a second append. The load is read-only
        // (#5833, revision 2, C1): it writes no row and never raises the entry to the
        // presented generation, so only an arm or a close re-claims.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.ShardRoot(3);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(initiator, 2, CancellationToken.None);
        var restarted = await CreateFencedGrainAsync(storage);
        await restarted.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        var first = await restarted.AppendBatchStampedAsync(new[] { MakeEntry("a") }, new WalAppendStamp(FenceLeaf, 1, initiator, 5), CancellationToken.None);
        var durable = (await WalAppendFenceRows.ReadGenerationAsync(storage, TreeId, ShardIndex, initiator)).State.Generation;
        var stillAdmitsDurable = await restarted.AppendStampedAsync(MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, initiator, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "the first append under an unloaded generation was not admitted");
            Assert.That(durable, Is.EqualTo(2), "UnloadedGenerationLoadsWithoutClaim: the cold path claimed the presented generation");
            Assert.That(stillAdmitsDurable.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted),
                "the cold path raised the entry to the presented generation, fencing the durable one's in-flight writes");
        });
    }

    [Test]
    public async Task A_fresh_partition_s_cold_load_writes_no_generation_row_and_admits_above_the_floor_only()
    {
        // #5833, revision 2, C1 and C7: an absent row loads the floor, floor-only. A generation at the
        // floor belongs to a purged incarnation and stays refused, as a claim at the floor
        // is (ColdLoadAdmitsAtFloor).
        var storage = new EnumerableMemoryGrainStorage();
        var initiator = WalRoutingInitiator.ShardRoot(3);
        await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, TreeId, new Dictionary<WalRoutingInitiator, long> { [initiator] = 4 });
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        var atFloor = await grain.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, initiator, 4), CancellationToken.None);
        var above = await grain.AppendStampedAsync(MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, initiator, 5), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(atFloor.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "a generation at the floor was admitted by the cold load");
            Assert.That(above.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty, "the cold load wrote a generation row");
        });
    }

    [Test]
    public async Task A_cold_load_never_lowers_a_generation_a_re_claim_raised()
    {
        // #5833, revision 2, C7 (ColdLoadLowersLoadedGeneration): a load whose read of the
        // row precedes a re-claim's raise must not install the older value after it. The
        // load's read is held after it has read, until the re-claim finishes or a bound
        // passes: under the claim gate the re-claim waits for the load, so the bound
        // releases it; without the gate the re-claim raises first, and only the load's
        // max-merge keeps the entry at the raised generation.
        var inner = new EnumerableMemoryGrainStorage();
        var initiator = WalRoutingInitiator.ShardRoot(3);
        var seed = await CreateFencedGrainAsync(inner);
        await seed.ClaimRoutingGenerationAsync(initiator, 1, CancellationToken.None);
        var storage = new HeldFirstReadStorage(inner, WalAppendFenceRows.GenerationGrainType);
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        var load = grain.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, initiator, 1), CancellationToken.None);
        await storage.FirstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var reclaim = grain.ClaimRoutingGenerationAsync(initiator, 3, CancellationToken.None);
        _ = reclaim.ContinueWith(_ => storage.Release.TrySetResult(), TaskScheduler.Default);
        await Task.WhenAll(load, reclaim);
        var old = await grain.AppendStampedAsync(MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, initiator, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reclaim.Result.Accepted, Is.True);
            Assert.That(old.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "a cold load lowered the re-claimed generation");
        });
    }

    [Test]
    public async Task A_close_between_the_append_s_generation_load_and_its_admission_rejects_it_as_stale()
    {
        // #5831 review, F2: the append's own load of its generation must be followed by
        // a fresh admission, never admit on the claim's result. A close that re-claims a
        // higher generation in between fences the append.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.ShardRoot(3);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var closes = 0;
        grain.AfterStampGenerationLoadedForTesting = async () =>
        {
            if (Interlocked.Increment(ref closes) == 1)
            {
                var close = await grain.ClaimRoutingGenerationAsync(initiator, 2, CancellationToken.None);
                Assert.That(close.Accepted, Is.True, "precondition: the close re-claimed the higher generation");
            }
        };
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        var single = await grain.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, initiator, 1), CancellationToken.None);
        var after = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(closes, Is.EqualTo(1), "the append did not load its generation itself");
            Assert.That(single.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "an append was admitted on its claim's result");
            Assert.That(after, Is.EqualTo(before), "a fenced append allocated an offset");
        });
    }

    [Test]
    public async Task A_batch_whose_generation_a_close_re_claims_after_its_load_is_rejected_unallocated()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.ShardRoot(3);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        grain.AfterStampGenerationLoadedForTesting = async () =>
            await grain.ClaimRoutingGenerationAsync(initiator, 2, CancellationToken.None);
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        var batch = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("a"), MakeEntry("b") }, new WalAppendStamp(FenceLeaf, 1, initiator, 1), CancellationToken.None);

        var after = await grain.GetNextSequenceAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(batch.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration));
            Assert.That(batch.AdmittedCount, Is.Zero);
            Assert.That(after, Is.EqualTo(before), "a fenced batch allocated an offset");
        });
    }

    [TestCase(1, 0)]
    [TestCase(2, 0)]
    [TestCase(3, 0)]
    [TestCase(2, 1)]
    [TestCase(3, 2)]
    [TestCase(1, 2)]
    public async Task The_memory_decision_of_a_re_presented_generation_equals_the_io_decision(long initial, long floor)
    {
        // #5831 review, F1: a claim decided from the value this activation loaded must
        // decide exactly as one that reads the row, for every value at or below it.
        var initiator = WalRoutingInitiator.ShardRoot(3);
        for (var value = 1L; value <= initial + 1; value++)
        {
            var fast = await ClaimTwiceAsync(initiator, initial, value, floor, fastPath: true);
            var io = await ClaimTwiceAsync(initiator, initial, value, floor, fastPath: false);
            Assert.That(fast, Is.EqualTo(io), $"initial {initial}, floor {floor}, re-claim {value}");
        }
    }

    private static async Task<(bool Accepted, long Head, long Refusal)> ClaimTwiceAsync(
        WalRoutingInitiator initiator, long initial, long value, long floor, bool fastPath)
    {
        var storage = new EnumerableMemoryGrainStorage();
        if (floor > 0)
        {
            await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, TreeId, initiator, floor);
        }

        var grain = await CreateFencedGrainAsync(storage);
        grain.DisableLoadedClaimFastPathForTesting = !fastPath;
        await grain.ClaimRoutingGenerationAsync(initiator, initial, CancellationToken.None);
        var result = await grain.ClaimRoutingGenerationAsync(initiator, value, CancellationToken.None);
        return (result.Accepted, result.Head, result.Refusal);
    }

    [TestCase(0, 0)]
    [TestCase(0, 2)]
    [TestCase(3, 0)]
    [TestCase(3, 2)]
    [TestCase(2, 3)]
    [TestCase(3, 3)]
    public async Task The_memory_decision_after_a_cold_load_equals_the_io_decision(long row, long floor)
    {
        // #5833 review of the C1 refinement: the grid of the F1 exactness test, over an
        // entry a read-only cold load installed - floor-only when the row is not above the
        // floor. Every re-claim decides exactly as one that reads the row, and admits
        // exactly what it would after reading it.
        var initiator = WalRoutingInitiator.ShardRoot(3);
        for (var value = 1L; value <= Math.Max(row, floor) + 2; value++)
        {
            var fast = await ClaimAfterColdLoadAsync(initiator, row, floor, value, fastPath: true);
            var io = await ClaimAfterColdLoadAsync(initiator, row, floor, value, fastPath: false);
            Assert.That(fast, Is.EqualTo(io), $"row {row}, floor {floor}, re-claim {value}");
        }
    }

    private static async Task<(bool Accepted, long Head, long Refusal, WalAppendFenceOutcome AtValue)> ClaimAfterColdLoadAsync(
        WalRoutingInitiator initiator, long row, long floor, long value, bool fastPath)
    {
        var storage = new EnumerableMemoryGrainStorage();
        if (floor > 0)
        {
            await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, TreeId, initiator, floor);
        }

        if (row > 0)
        {
            var stored = await WalAppendFenceRows.ReadGenerationAsync(storage, TreeId, ShardIndex, initiator);
            stored.State.Generation = row;
            await WalAppendFenceRows.WriteAsync(
                storage,
                WalAppendFenceRows.GenerationGrainType,
                WalAppendFenceRows.GenerationKey(TreeId, ShardIndex, initiator.Kind, initiator.Index),
                stored);
        }

        var grain = await CreateFencedGrainAsync(storage);
        grain.DisableLoadedClaimFastPathForTesting = !fastPath;
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var load = await grain.AppendStampedAsync(
            MakeEntry("load"), new WalAppendStamp(FenceLeaf, 1, initiator, Math.Max(row, floor) + 1), CancellationToken.None);
        Assert.That(load.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "precondition: the cold load admitted above the row and floor");

        var result = await grain.ClaimRoutingGenerationAsync(initiator, value, CancellationToken.None);
        var atValue = await grain.AppendStampedAsync(MakeEntry("probe"), new WalAppendStamp(FenceLeaf, 1, initiator, value), CancellationToken.None);
        return (result.Accepted, result.Head, result.Refusal, atValue.Outcome);
    }

    [Test]
    public async Task A_re_claim_after_reactivation_reads_no_directory_page_when_its_entry_exists()
    {
        // #5831: only a leaf's first activation (epoch 1) lists itself in parallel with
        // reading its entry. A reactivated leaf's re-claim finds its entry, which was
        // listed before it was first written, so it must not pay a directory read.
        var storage = new ReadCountingStorage(new EnumerableMemoryGrainStorage());
        var first = await CreateFencedGrainAsync(storage);
        Assert.That((await first.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None)).Accepted, Is.True);
        var listedOnFirstClaim = storage.Reads(WalAppendFenceRows.DirectoryPageGrainType);

        var restarted = await CreateFencedGrainAsync(storage);
        storage.Reset();
        var reclaim = await restarted.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(listedOnFirstClaim, Is.GreaterThan(0), "precondition: the first claim listed the leaf");
            Assert.That(reclaim.Accepted, Is.True);
            Assert.That(storage.Reads(WalAppendFenceRows.DirectoryPageGrainType), Is.Zero,
                "a re-claim whose entry exists read the directory");
            Assert.That(storage.Reads(WalAppendFenceRows.EntryGrainType), Is.EqualTo(1));
        });
    }

    /// <summary>Counts reads per grain type.</summary>
    private sealed class ReadCountingStorage(IGrainStorage inner) : IGrainStorage
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _reads = new();

        public int Reads(string grainType) => _reads.TryGetValue(grainType, out var count) ? count : 0;

        public void Reset() => _reads.Clear();

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _reads.AddOrUpdate(stateName, 1, (_, v) => v + 1);
            return inner.ReadStateAsync(stateName, grainId, grainState);
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }

    /// <summary>
    /// Delays reads of one grain type, so concurrent claims all read before any of
    /// them writes, and counts that type's writes and write conflicts.
    /// </summary>
    /// <summary>
    /// Holds the first read of <paramref name="grainType"/> after it has read, until
    /// <see cref="Release"/> or a bound, so a concurrent writer can move the row between
    /// the read and its caller's use of it.
    /// </summary>
    private sealed class HeldFirstReadStorage(IGrainStorage inner, string grainType) : IGrainStorage
    {
        private int _reads;

        public TaskCompletionSource FirstReadDone { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            await inner.ReadStateAsync(stateName, grainId, grainState);
            if (stateName == grainType && Interlocked.Increment(ref _reads) == 1)
            {
                FirstReadDone.TrySetResult();
                await Task.WhenAny(Release.Task, Task.Delay(TimeSpan.FromMilliseconds(500)));
            }
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }

    private sealed class SlowRowStorage(IGrainStorage inner, string grainType) : IGrainStorage
    {
        private int _writes;
        private int _conflicts;

        public int Writes => Volatile.Read(ref _writes);

        public int Conflicts => Volatile.Read(ref _conflicts);

        public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (stateName == grainType)
            {
                await Task.Delay(50);
            }

            await inner.ReadStateAsync(stateName, grainId, grainState);
        }

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            try
            {
                await inner.WriteStateAsync(stateName, grainId, grainState);
                if (stateName == grainType)
                {
                    Interlocked.Increment(ref _writes);
                }
            }
            catch (InconsistentStateException) when (stateName == grainType)
            {
                Interlocked.Increment(ref _conflicts);
                throw;
            }
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }
}
