using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Source_confirmation_consumes_the_captured_replacement_horizon_before_omitting_a_key(
        bool advanceHeadDuringSourceRead)
    {
        var view = new FakeViewTree();
        var state = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, state);
        var transaction = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(transaction, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(transaction, OtherKey, 2, ticks: 20, size: 2, index: 1),
            Commit(transaction, ticks: 21),
        ];
        ArmLog(h, log);
        h.SourceRows[Key] = [3];
        h.SourceVersions[Key] = Clock(30);
        h.HeadOffset.Value = log.Count + (advanceHeadDuringSourceRead ? 0 : 1);
        var sourceObserved = false;
        if (advanceHeadDuringSourceRead)
        {
            h.SourceRoot.GetVisibleRawEntriesAsync(Arg.Any<List<string>>()).Returns(call =>
            {
                var rows = call.ArgAt<List<string>>(0)
                    .Select(key => (LwwEntry?)new LwwEntry
                    {
                        Key = key,
                        Value = h.SourceRows[key],
                        Timestamp = h.SourceVersions[key],
                    })
                    .ToList();
                if (!sourceObserved)
                {
                    h.HeadOffset.Value = log.Count + 1;
                    sourceObserved = true;
                }

                return Task.FromResult(rows);
            });
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Snapshots, Is.Empty,
                "The logical replacement is visible at source, but its WAL event is beyond consumed progress.");
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
            Assert.That(sourceObserved, Is.EqualTo(advanceHeadDuringSourceRead));
        });

        log.Add(OrdinaryAt(Key, 3, ticks: 30));
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Snapshots.Where(snapshot => snapshot.ContainsKey(OtherKey))
                .Select(snapshot => snapshot.GetValueOrDefault(Key)?.FirstOrDefault()),
                Is.All.EqualTo((byte)3),
                "The omitted key's actual replacement must precede publication of the remaining batch.");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }));
            Assert.That(state.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_transitive_ready_closure_revisits_earlier_candidates_and_replays_one_complete_effect(
        bool reactivateAfterLostResponse)
    {
        var view = new FakeViewTree { LoseNextAtomicResponse = reactivateAfterLostResponse };
        var state = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, state);
        // Equal maximum HLCs use transaction-ID order, placing the far member before its bridges.
        var first = Guid.Parse("00000001-0000-0000-0000-000000000000");
        var far = Guid.Parse("00000002-0000-0000-0000-000000000000");
        var returning = Guid.Parse("00000003-0000-0000-0000-000000000000");
        var bridge = Guid.Parse("00000004-0000-0000-0000-000000000000");
        List<LatticeMutation> log =
        [
            Prepared(first, Key, 1, ticks: 10, size: 2, index: 0),
            Prepared(first, OtherKey, 1, ticks: 30, size: 2, index: 1),
            Commit(first, ticks: 31),
            Prepared(far, "m", 3, ticks: 10, size: 2, index: 0),
            Prepared(far, "n", 3, ticks: 30, size: 2, index: 1),
            Commit(far, ticks: 31),
            Prepared(returning, "n", 4, ticks: 10, size: 2, index: 0),
            Prepared(returning, Key, 4, ticks: 30, size: 2, index: 1),
            Commit(returning, ticks: 31),
            Prepared(bridge, OtherKey, 2, ticks: 10, size: 2, index: 0),
            Prepared(bridge, "m", 2, ticks: 30, size: 2, index: 1),
            Commit(bridge, ticks: 31),
        ];
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        Assert.That(view.Snapshots, Is.Empty,
            "Every ring member first needs the consumed post-confirmation source horizon.");

        if (reactivateAfterLostResponse)
        {
            Assert.ThrowsAsync<TimeoutException>(() => h.Grain.DrainAsync());
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1),
                "A lost publication response cannot certify durable subscription progress.");
            h = Create(view, state);
            ArmLog(h, log);
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Snapshots, Has.Count.EqualTo(1),
                "The bridge is visited after the far candidate, so transitive closure must revisit it.");
            Assert.That(view.Snapshots[0].Keys, Is.EquivalentTo(new[] { Key, OtherKey, "m", "n" }));
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 4 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 1 }));
            Assert.That(view.Rows["m"], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows["n"], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.CompletedOperations, Has.Count.EqualTo(1));
            Assert.That(view.AtomicOperations.Distinct().ToArray(), Has.Length.EqualTo(1),
                "Reactivation reattaches the same complete publication identity.");
            Assert.That(view.PointWrites, Is.Empty);
            Assert.That(state.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }
}
