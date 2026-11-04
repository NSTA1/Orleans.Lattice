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
