using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

// Class B regression tests for LatticeBootstrapCoordinatorGrain. The grain
// has TWO WriteStateAsync sites with observable failure modes under realistic
// single-write-throw conditions:
//
//   * Site 1 - TryInitiateBootstrapAsync kickoff persist (L141): no
//     surrounding catch. The `if (state.State.InProgress)` guard at L117
//     short-circuits the next same-source kickoff retry, silently dropping
//     the bootstrap until the activation recycles.
//
//   * Site 3 - ProcessNextPhaseAsync catch-handler persist (L189): the L174
//     outer catch sets Phase=Failed and InProgress=false before persisting
//     at L189. If L189 throws, the catch's `persisted=false` branch
//     (L194-198) deliberately leaves the keepalive armed so the next tick
//     retries the Failed pivot - but the dirty in-memory `InProgress=false`
//     makes the `if (!state.State.InProgress) return;` guard at L148
//     short-circuit every retry from the same activation, silently breaking
//     the author's documented retry intent.
//
// Sites 2 (default-branch terminal teardown at L168-L169), 4 (DrainSnapshotAsync
// initial cursor at L237-L243), 5 (per-batch cursor at L271-L278), 6
// (IncrementalHandoff transition at L283-L284), and 7 (PinAndCompleteAsync
// terminal at L307-L309) are masked by the L174 catch handler's always-
// successful L189 launder: any dirty Phase / InProgress they leave behind is
// overridden at L184/L185 and persisted by L189. Their dirty windows are
// unobservable at the public boundary under one-shot throw conditions, so
// they are bundled as benign-with-reason in the pattern sweep rather than
// fixed.
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public void TryInitiateBootstrap_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _, _) = Create();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.TryInitiateBootstrapAsync(SourceCluster),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // Without the revert, the same-source retry observes
        // `state.State.InProgress=true` at L117 and returns false; a
        // different-source retry throws "already in progress". The bootstrap
        // is silently dropped until the activation recycles. All 7 fields
        // mutated at L134-L140 must revert to their pre-call defaults.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False,
                "InProgress must revert so the next kickoff can run the persist");
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.Idle),
                "Phase must revert from RequestingSnapshot so GetStateAsync does not lie");
            Assert.That(state.State.SourceClusterId, Is.EqualTo(string.Empty),
                "SourceClusterId must revert so a different-source kickoff is not rejected");
            Assert.That(state.State.OperationId, Is.EqualTo(string.Empty),
                "OperationId must revert; a stale id leaks into diagnostics");
            Assert.That(state.State.LastAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(state.State.SnapshotAsOfHlc, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(state.State.CausalStableFrontier.Entries, Is.Empty,
                "CausalStableFrontier must revert to its POCO-initialized empty value");
        });
    }

    [Test]
    public void ProcessNextPhase_failed_pivot_reverts_state_when_catch_handler_WriteStateAsync_throws()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, phase: LatticeBootstrapState.ApplyingSnapshot);
        var (grain, _, _, provider, reminders, _, _) = Create(fake);
        // ExportAsync throws so DrainSnapshotAsync throws *before* any
        // state.WriteStateAsync call - control reaches the L174 catch
        // without first consuming the one-shot ThrowOnWrite.
        provider.ExportAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("export boom"));
        // Make the catch-handler's L189 persist throw - the one site where a
        // failed write at L189 silently breaks the L207 "leave keepalive
        // armed for retry" path because dirty in-memory `InProgress=false`
        // short-circuits L148 on every retry tick.
        fake.ThrowOnWrite = new InvalidOperationException("write boom");
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(async () => await grain.ProcessNextPhaseAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("export boom"));

        // Without the revert, in-memory has InProgress=false and Phase=Failed
        // (set at L184/L185 then *not* persisted because L189 threw). Every
        // subsequent ProcessNextPhase tick short-circuits at L148, silently
        // breaking the documented "leave keepalive armed for retry" intent.
        Assert.Multiple(() =>
        {
            Assert.That(fake.State.InProgress, Is.True,
                "InProgress must revert to true so the next phase tick can retry the Failed pivot");
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.ApplyingSnapshot),
                "Phase must revert to ApplyingSnapshot so retry resumes from the seeded phase");
        });
    }
}
