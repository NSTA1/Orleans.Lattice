using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public sealed class ReplicationExportEpochGrainTests
{
    private static (ReplicationExportEpochGrain Grain, FakePersistentState<ReplicationExportEpochState> State) CreateGrain(long epoch)
    {
        var state = new FakePersistentState<ReplicationExportEpochState>();
        state.State.Epoch = epoch;
        return (new ReplicationExportEpochGrain(state), state);
    }

    private static ReplicationExportCut Cut(long epoch, params long[] heads) =>
        new() { Epoch = epoch, WalTreeId = "wal", OpenHeads = heads };

    [Test]
    public async Task RecordCutAsync_ignores_non_positive_future_and_duplicate_epochs()
    {
        var (grain, state) = CreateGrain(epoch: 3);

        await grain.RecordCutAsync(Cut(0, 5));
        await grain.RecordCutAsync(Cut(4, 5));
        await grain.RecordCutAsync(Cut(2, 5));
        await grain.RecordCutAsync(Cut(2, 9));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Cuts.Select(c => c.Epoch), Is.EqualTo(new long[] { 2 }));
            Assert.That(state.State.Cuts[0].OpenHeads, Is.EqualTo(new long[] { 5 }));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RecordCutAsync_keeps_cuts_sorted_and_bounded()
    {
        var (grain, state) = CreateGrain(epoch: 20);
        for (var e = 20L; e >= 1; e--)
        {
            await grain.RecordCutAsync(Cut(e, 1));
        }

        await grain.RecordCutAsync(Cut(12, 1));

        Assert.That(state.State.Cuts.Select(c => c.Epoch),
            Is.EqualTo(Enumerable.Range(13, ReplicationExportEpochGrain.MaxRecordedCuts).Select(i => (long)i)));
    }

    [Test]
    public async Task RecordCutAsync_failed_write_rolls_back()
    {
        var (grain, state) = CreateGrain(epoch: 2);
        await grain.RecordCutAsync(Cut(1, 1));
        state.ThrowOnWrite = new IOException("boom");

        Assert.ThrowsAsync<IOException>(() => grain.RecordCutAsync(Cut(2, 1)));
        Assert.That(state.State.Cuts.Select(c => c.Epoch), Is.EqualTo(new long[] { 1 }));
    }

    [Test]
    public async Task GetEarliestCoveringEpochAsync_returns_lowest_contiguous_covering_epoch()
    {
        var (grain, _) = CreateGrain(epoch: 5);
        await grain.RecordCutAsync(Cut(2, 10, 10));
        await grain.RecordCutAsync(Cut(4, 3, 4));
        await grain.RecordCutAsync(Cut(5, 7, 7));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", [3, 4]), Is.EqualTo(4L),
                "epoch 3 is missing, so the walk stops at 4");
            Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", [5, 5]), Is.EqualTo(5L),
                "epoch 4's cut trimmed below the retained heads");
            Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", [8, 1]), Is.Null);
            Assert.That(await grain.GetEarliestCoveringEpochAsync("other", [1, 1]), Is.Null);
            Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", [1]), Is.Null);
            Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", []), Is.Null);
        });
    }

    [Test]
    public async Task GetEarliestCoveringEpochAsync_requires_the_current_epoch_to_be_recorded()
    {
        var (grain, state) = CreateGrain(epoch: 3);
        await grain.RecordCutAsync(Cut(2, 9));
        await grain.RecordCutAsync(Cut(3, 9));
        state.State.Epoch = 4;

        Assert.That(await grain.GetEarliestCoveringEpochAsync("wal", [1]), Is.Null);
    }
}
