using NSubstitute;
using NSubstitute.Core;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4604: a snapshot entry the replication applier defers
/// (<see cref="ApplyResult.Deferred"/>) - whatever deferred it: a coordinated
/// restore's receive fence, an in-flight duplicate, a restored copy's fence - is
/// not dropped. The drain stops at it without counting it or folding its clock
/// into the handoff seal, re-drains within the transient-retry budget, and
/// otherwise fails with the import started so the read fence stays up and the
/// bootstrap is re-driven. It never reaches the handoff past the entry.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static Func<CallInfo, Task<ApplyResult>> DeferKey(string deferredKey, Func<bool>? stillDeferred = null) =>
        call =>
        {
            var record = (WalRecord)call[0];
            var defer = record.Key == deferredKey && (stillDeferred?.Invoke() ?? true);
            return Task.FromResult(defer
                ? new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero, Deferred = true }
                : new ApplyResult { Applied = true, HighWaterMark = record.Timestamp });
        };

    private static SnapshotEntry[] ThreeEntries() =>
    [
        new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
        new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(5) },
        new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = Hlc(3) },
    ];

    [Test]
    public async Task A_deferred_snapshot_entry_fails_the_drain_with_the_fence_kept_and_never_reaches_the_handoff()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, hwm, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(ThreeEntries()))));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(DeferKey("b"));

        Assert.ThrowsAsync<LatticeBootstrapEntryDeferredException>(() => grain.ProcessNextPhaseAsync());

        Assert.Multiple(() =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed),
                "the drain must not complete past an entry it did not apply");
            Assert.That(fake.State.InProgress, Is.True, "the bootstrap stays in progress to be re-driven");
            Assert.That(fake.State.ReadFenceArmed, Is.True, "the partial import stays read-fenced");
            Assert.That(fake.State.NextRedriveAtUtcTicks, Is.GreaterThan(0), "a re-drive is scheduled");
            Assert.That(fake.State.EntriesApplied, Is.EqualTo(1), "the deferred entry is not counted as applied");
            Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(1)),
                "the deferred entry's clock is not folded into the handoff seal");
        });
        await apply.DidNotReceive().ApplyAsync(Arg.Is<WalRecord>(r => r.Key == "c"), Arg.Any<CancellationToken>());
        await hwm.DidNotReceiveWithAnyArgs().MergeBootstrapFrontierAsync(default, default!, default);
    }

    [Test]
    public async Task A_deferred_snapshot_entry_is_re_drained_within_the_retry_budget_even_when_the_host_classifier_rejects_it()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, apply, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 2, classifier: _ => false));
        var exports = 0;
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                exports++;
                return Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(ThreeEntries())));
            });
        var deferrals = 0;
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(DeferKey("b", stillDeferred: () => deferrals++ == 0));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(exports, Is.EqualTo(2), "the deferral re-opened the full snapshot once");
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(fake.State.EntriesApplied, Is.EqualTo(3), "the re-drain applied every entry");
            Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(5)));
        });
    }

    [Test]
    public void LatticeBootstrapEntryDeferredException_names_the_tree_and_the_key()
    {
        var ex = new LatticeBootstrapEntryDeferredException("tree-x", "key-y");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeName, Is.EqualTo("tree-x"));
            Assert.That(ex.Key, Is.EqualTo("key-y"));
            Assert.That(ex.Message, Does.Contain("tree-x").And.Contain("key-y"));
        });
    }
}
