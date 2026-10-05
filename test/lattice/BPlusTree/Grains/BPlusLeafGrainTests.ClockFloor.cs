using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A leaf's handling of a WAL partition's clock-floor refusal (issue #4586): a
/// single-key commit re-stamps above the floor and commits once; a stamp the
/// leaf did not choose, or a commit that cannot be re-run, propagates the
/// refusal with the leaf clock merged past the floor.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly HybridLogicalClock FutureFloor =
        new() { WallClockTicks = DateTimeOffset.UtcNow.AddHours(1).UtcTicks, Counter = 0 };

    /// <summary>
    /// A writer whose first single append is refused below <see cref="FutureFloor"/>;
    /// every later append, single or batched, is recorded and succeeds unless
    /// <paramref name="refuseBatches"/> is set.
    /// </summary>
    private static ICommitLogWriter CreateRefusingOnceWriter(List<WalRecord> appended, bool refuseBatches = false)
    {
        var refused = false;
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var record = (WalRecord)callInfo[0];
            if (!refused && record.Timestamp < FutureFloor)
            {
                refused = true;
                return Task.FromException<long>(new WalStampBelowFloorException(record.TreeId, 0, record.Timestamp, FutureFloor));
            }

            appended.Add(record);
            return Task.FromResult((long)appended.Count - 1);
        });
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            var records = (IReadOnlyList<WalRecord>)callInfo[0];
            if (refuseBatches && records.Any(r => r.Timestamp < FutureFloor))
            {
                return Task.FromException<IReadOnlyList<long>>(
                    new WalStampBelowFloorException(records[0].TreeId, 0, records[0].Timestamp, FutureFloor));
            }

            appended.AddRange(records);
            return Task.FromResult<IReadOnlyList<long>>(new long[records.Count]);
        });
        return writer;
    }

    [Test]
    public async Task SetAsync_restamps_above_a_refused_clock_floor_and_commits_once()
    {
        var appended = new List<WalRecord>();
        var grain = CreateGrain(commitLog: CreateRefusingOnceWriter(appended));

        await grain.SetAsync("k", new byte[] { 1 });

        Assert.Multiple(() =>
        {
            Assert.That(appended, Has.Count.EqualTo(1), "the refused attempt appended nothing");
            Assert.That(appended[0].Timestamp, Is.GreaterThan(FutureFloor));
            Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(appended[0].Timestamp),
                "the stored value carries the re-stamped HLC the WAL holds");
        });
    }

    [Test]
    public async Task DeleteAsync_restamps_above_a_refused_clock_floor_and_commits_once()
    {
        var appended = new List<WalRecord>();
        var grain = CreateGrain(commitLog: CreateRefusingOnceWriter(appended));
        await grain.SetAsync("k", new byte[] { 1 });
        appended.Clear();

        var deleted = await grain.DeleteAsync("k");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(appended, Has.Count.EqualTo(1));
            Assert.That(appended[0].Op, Is.EqualTo(MutationKind.Delete));
            Assert.That(grain.EntriesForTest["k"].IsTombstone, Is.True);
            Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(appended[0].Timestamp));
        });
    }

    [Test]
    public async Task A_refused_override_stamp_propagates_without_a_restamp()
    {
        var appended = new List<WalRecord>();
        var grain = CreateGrain(commitLog: CreateRefusingOnceWriter(appended));
        var overrideStamp = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };

        using (LatticeHlcOverrideContext.With(overrideStamp))
        {
            Assert.ThrowsAsync<WalStampBelowFloorException>(async () => await grain.SetAsync("k", new byte[] { 1 }));
        }

        Assert.Multiple(() =>
        {
            Assert.That(appended, Is.Empty);
            Assert.That(grain.EntriesForTest.ContainsKey("k"), Is.False, "nothing was applied");
        });
    }

    [Test]
    public async Task A_refused_batch_propagates_with_the_leaf_clock_merged_past_the_floor()
    {
        var appended = new List<WalRecord>();
        var grain = CreateGrain(commitLog: CreateRefusingOnceWriter(appended, refuseBatches: true));

        Assert.ThrowsAsync<WalStampBelowFloorException>(async () => await grain.SetManyAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("a", new byte[] { 1 }),
            new("b", new byte[] { 2 }),
        }));

        Assert.That(await grain.GetClockAsync(), Is.GreaterThan(FutureFloor),
            "the caller's retry mints stamps above the floor");
    }
}
