using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay(
        bool loseResponse)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var codec = new HistoryRowCodec(services.GetRequiredService<Serializer<HistoryRow>>());
        var projection = new HistoryLatticeViewProjection(codec);
        var registration = new ViewRegistration(ViewName, SourceTreeId, projection, Accumulative: true);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var log = new List<LatticeMutation>
        {
            Prepared(first, Key, 2, 20, 2, 0),
            Prepared(first, "j", 2, 40, 2, 1),
            Ordinary(5, 50),
            Prepared(second, Key, 3, 30, 2, 0),
            Prepared(second, "j", 3, 10, 2, 1),
            Ordinary(1, 5),
            Commit(first, 41),
            Commit(second, 31),
        };
        var expected = log.Where(m => m.Kind == MutationKind.Set)
            .Select(m => HistoryKey.Encode(m.Key, m.Timestamp)).ToArray();
        var view = new FakeViewTree { LoseNextAtomicResponse = loseResponse };
        var state = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, state, registration: registration, historyCodec: codec);
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;

        if (loseResponse)
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.DrainAsync());
            h = Create(view, state, registration: registration, historyCodec: codec);
            ArmLog(h, log);
            h.HeadOffset.Value = log.Count;
        }

        await h.Grain.DrainAsync();

        Assert.That(view.Rows.Keys, Is.EquivalentTo(expected),
            "Accumulative admission records every committed revision, not just current source winners (#4885/#4886).");
        Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(log.Count - 1));
        var before = view.Rows.Keys.ToArray();
        await h.Grain.DrainAsync();
        Assert.That(view.Rows.Keys, Is.EquivalentTo(before));
    }
}
