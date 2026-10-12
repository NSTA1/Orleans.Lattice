using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewCrossTreeCoordinatorGrainTests
{
    private static ViewCrossTreeCoordinatorState LegacyDecidedState() => new()
    {
        WaitSet = ["history", "peer"],
        StartedAtTicks = DateTime.UtcNow.AddMinutes(-1).Ticks,
        Decided = true,
        Slices = new(StringComparer.Ordinal)
        {
            ["history"] = new()
            {
                ViewName = "history",
                ViewTreeId = "original-history-generation",
                Upserts =
                [
                    new("entry/revision-1", [1]),
                    new("entry/revision-2", [2]),
                ],
                Deletes = [],
            },
            ["peer"] = new()
            {
                ViewName = "peer",
                ViewTreeId = "original-peer-generation",
                Upserts = [],
                Deletes = [],
            },
        },
    };

    [TestCase(false)]
    [TestCase(true)]
    public async Task Upgrade_legacy_decided_replays_frozen_history_after_lost_response_or_completion_write(
        bool completionWriteFails)
    {
        var persisted = LegacyDecidedState();
        var state = new FakePersistentState<ViewCrossTreeCoordinatorState> { State = persisted };
        state.OnWriteState = value => persisted = JsonSerializer.Deserialize<ViewCrossTreeCoordinatorState>(
            JsonSerializer.Serialize(value))!;
        var h = CreateGrain(state);
        var target = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("original-history-generation").Returns(target);
        var attempts = 0;
        var publications = new List<(string Identity, string[] Keys, byte[][] Values)>();
        target.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entries = call.ArgAt<List<KeyValuePair<string, byte[]>>>(0);
                publications.Add((call.ArgAt<string>(2), entries.Select(entry => entry.Key).ToArray(),
                    entries.Select(entry => entry.Value.ToArray()).ToArray()));
                if (++attempts == 1)
                {
                    if (!completionWriteFails)
                    {
                        return Task.FromException(new TimeoutException("Legacy publication response lost."));
                    }

                    state.ThrowOnWrite = new TimeoutException("Legacy completion write lost.");
                }

                return Task.CompletedTask;
            });

        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RegisterReadyAsync(
            Readiness("history", "replacement-generation", ["history", "peer"], Upsert("winner-only", 99))));
        Assert.Multiple(() =>
        {
            Assert.That(persisted.Decided, Is.True);
            Assert.That(persisted.Applied, Is.False);
            Assert.That(persisted.RepairSlices, Is.Empty);
        });

        var restarted = CreateGrain(
            new FakePersistentState<ViewCrossTreeCoordinatorState> { State = persisted }, h.GrainFactory);
        var decision = await restarted.Grain.RegisterDegradedAsync("history");
        Assert.Multiple(() =>
        {
            Assert.That(decision.Applied, Is.True, "A decided legacy publication must finish automatically.");
            Assert.That(decision.Degraded, Is.False);
            Assert.That(decision.RequiresRepair, Is.False);
            Assert.That(publications, Has.Count.EqualTo(2));
            Assert.That(publications.Select(publication => publication.Identity),
                Is.EqualTo(new[] { "mv-xt-op-xt-1", "mv-xt-op-xt-1" }));
            foreach (var publication in publications)
            {
                Assert.That(publication.Keys, Is.EqualTo(new[] { "entry/revision-1", "entry/revision-2" }),
                    "Current-source winner rebuilding must not erase shipped history revisions.");
                Assert.That(publication.Values, Is.EqualTo(new byte[][] { [1], [2] }));
            }

            Assert.That(restarted.State.State.RepairApplied, Is.False,
                "Old-contract completion is not a new-protocol repair certificate.");
            Assert.That(restarted.State.State.RepairAcknowledged, Is.Empty);
            Assert.That(restarted.State.State.RepairProtocolVersion, Is.Zero);
        });

        await restarted.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(restarted.State.RecordExists, Is.False,
            "Completed legacy work must retain automatic shipped retention, not require nonexistent peer repair.");
    }

    [Test]
    public async Task Upgrade_legacy_completed_without_source_context_does_not_rebuild_or_require_peer_acknowledgements()
    {
        var legacy = LegacyDecidedState();
        legacy.Applied = true;
        legacy.Slices.Clear();
        var h = CreateGrain(new FakePersistentState<ViewCrossTreeCoordinatorState> { State = legacy });

        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("history", "unrelated-current-generation", ["history", "peer"], Upsert("winner-only", 99)));
        Assert.Multiple(() =>
        {
            Assert.That(decision.Applied, Is.True);
            Assert.That(decision.RequiresRepair, Is.False);
            Assert.That(h.State.State.RepairApplied, Is.False);
            Assert.That(h.State.State.RepairSlices, Is.Empty);
            Assert.That(h.GrainFactory.ReceivedCalls(), Is.Empty,
                "Finished released work cannot acquire invented original source keys or rewrite later history.");
        });

        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(h.State.RecordExists, Is.False);
    }

    [Test]
    public async Task Upgrade_legacy_degraded_reactivation_preserves_terminal_per_slice_contract()
    {
        var legacy = LegacyDecidedState();
        legacy.Decided = false;
        legacy.Degraded = true;
        var h = CreateGrain(new FakePersistentState<ViewCrossTreeCoordinatorState> { State = legacy });

        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("history", "unrelated-current-generation", ["history", "peer"], Upsert("winner-only", 99)));
        Assert.Multiple(() =>
        {
            Assert.That(decision.Applied, Is.False);
            Assert.That(decision.Degraded, Is.True);
            Assert.That(decision.RequiresRepair, Is.False);
            Assert.That(h.State.State.RepairApplied, Is.False);
            Assert.That(h.GrainFactory.ReceivedCalls(), Is.Empty);
        });

        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(h.State.RecordExists, Is.False);
    }
}
