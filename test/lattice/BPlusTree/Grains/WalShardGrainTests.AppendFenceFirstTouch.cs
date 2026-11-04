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
        // loaded loads that generation itself, as the writer's re-presentation would,
        // rather than rejecting it and costing the writer a separate claim call and a
        // second append.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.ShardRoot(3);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var stamp = new WalAppendStamp(FenceLeaf, 1, initiator, 1);

        var first = await grain.AppendBatchStampedAsync(new[] { MakeEntry("a") }, stamp, CancellationToken.None);
        var durable = (await WalAppendFenceRows.ReadGenerationAsync(storage, TreeId, ShardIndex, initiator)).State.Generation;

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "the first append under an unloaded generation was not admitted");
            Assert.That(durable, Is.EqualTo(1), "the generation was admitted without being claimed durably");
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
