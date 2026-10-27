using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(MutationKind.Set, false)]
    [TestCase(MutationKind.Delete, false)]
    [TestCase(MutationKind.Tombstone, false)]
    [TestCase(MutationKind.Set, true)]
    [TestCase(MutationKind.Delete, true)]
    public async Task Resident_replay_does_not_restore_rows_moved_by_an_interleaved_split(
        MutationKind kind, bool prepared)
    {
        var wal = new GrowingWal();
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        var moved = new Dictionary<string, LwwValue<byte[]>>();
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(call =>
            {
                foreach (var (key, value) in call.Arg<Dictionary<string, LwwValue<byte[]>>>())
                    moved[key] = value;
                return Task.FromResult<SplitResult?>(null);
            });
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            persistedCheckpoint: -1,
            maxLeafKeys: 4,
            configureGrainFactory: factory =>
            {
                factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
                factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
            });
        await ActivateAsync(grain);
        for (var i = 1; i <= 4; i++)
            await grain.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));
        var transactionId = prepared ? Guid.NewGuid() : Guid.Empty;
        wal.GrowTo(prepared ? 6 : 5, offset => new LatticeMutation
        {
            TreeId = MaterialiserTreeId,
            Kind = offset == 6 ? MutationKind.TxCommit : kind,
            Key = offset == 6 ? string.Empty : $"k{offset}",
            Value = kind == MutationKind.Set ? Encoding.UTF8.GetBytes($"v{offset}") : null,
            IsTombstone = kind is MutationKind.Delete or MutationKind.Tombstone,
            IsPrepared = prepared && offset != 6,
            TransactionId = transactionId,
            Timestamp = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.AddDays(1).Ticks },
        });

        var reading = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        wal.OnRead = () =>
        {
            reading.TrySetResult();
            return release.Task;
        };
        var drive = grain.DriveStarvedCheckpointAsync();
        await reading.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            // The read already selected the old ownership window. Complete a
            // real division before delivering that slice to the resident replay.
            var split = await grain.SetAsync("k5", Encoding.UTF8.GetBytes("v5"));
            Assert.Multiple(() =>
            {
                Assert.That(split, Is.Not.Null);
                Assert.That(state.State.HighKeyExclusive, Is.EqualTo("k3"));
                Assert.That(moved.Keys, Is.EquivalentTo(new[] { "k3", "k4", "k5" }));
                Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k1", "k2" }));
            });
        }
        finally
        {
            release.TrySetResult();
            await drive;
        }

        var topology = await grain.GetTopologyNodeAsync();
        var digest = await grain.GetProjectionDigestAsync();
        var fullRange = await grain.GetProjectionDigestForRangeAsync(null, null);
        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k1", "k2" }),
                "A resident replay must not resurrect the sibling's three transferred rows.");
            Assert.That(topology.EntryCount + moved.Count, Is.EqualTo(5));
            Assert.That(digest.EntryCount, Is.EqualTo(2));
            Assert.That(fullRange.EntryCount, Is.EqualTo(2));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(wal.Head),
                "The split's checkpoint hint remains banked while transferred records are consumed.");
            Assert.That(grain.EntriesForTest.Values.All(value => value.IsTombstone),
                Is.EqualTo(kind == MutationKind.Delete),
                "The retained half must still apply its sets, tombstones, and transaction terminal.");
        });
    }

    [TestCase(null, "k3", null, new[] { "k3", "k4", "k5" })]
    [TestCase(null, null, "k3", new[] { "k1", "k2" })]
    [TestCase("k3", null, null, new[] { "k1", "k2" })]
    public async Task Resident_replay_intersects_captured_and_live_spans_after_a_suspended_read(
        string? initialHigh, string? liveLow, string? liveHigh, string[] expectedKeys)
    {
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            seedState: state => state.HighKeyExclusive = initialHigh);
        await ActivateAsync(grain);
        wal.GrowTo(5);
        var reading = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        wal.OnRead = () =>
        {
            reading.TrySetResult();
            return release.Task;
        };
        var drive = grain.DriveStarvedCheckpointAsync();
        await reading.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await grain.SetKeyRangeAsync(liveLow, liveHigh);
        }
        finally
        {
            release.TrySetResult();
            await drive;
        }

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(expectedKeys),
                "Narrowing rejects moved keys; widening cannot admit routing-only records excluded by the captured filter.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5));
        });
    }
}
