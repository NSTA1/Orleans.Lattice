using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Class B regression coverage: a transient WriteStateAsync failure on
/// any of the three cadence-stamp sites in ProcessNextPhaseAsync must
/// NOT latch the in-memory cadence stamp at the just-attempted
/// nowTicks value. The XML doc at L101-105 of ReplicationMaintenanceGrain
/// explicitly promises "the cadence stamp advances only on a successful
/// pass so a thrown GC retries on the next phase tick rather than
/// waiting a full cadence" - the snapshot/restore pattern in the grain
/// is what backs that promise. Pre-fix, the stamp was mutated before
/// the persist and the throw left the in-memory value dirty, causing
/// the very next phase tick's ShouldRunCadence guard to skip the work
/// for the full MaintenanceGcInterval / MaintenanceFallOffCheckInterval.
/// </summary>
public partial class ReplicationMaintenanceGrainTests
{
    [Test]
    public async Task ProcessNextPhase_reverts_LastGcTicks_when_WriteStateAsync_throws()
    {
        // Seed the other two cadence stamps to a recent value so only
        // the GC block fires this tick - prevents the orphan-sweep /
        // fall-off blocks' own WriteStateAsync calls from laundering
        // the dirty LastGcTicks back to disk under one-shot ThrowOnWrite.
        var sentinel = DateTime.UtcNow.Ticks;
        var seed = new ReplicationMaintenanceState
        {
            LastOrphanSweepTicks = sentinel,
            LastFallOffCheckTicks = sentinel,
        };
        var (grain, state, _, _, _, _, _, _) = Create(seed: seed);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await grain.ProcessNextPhaseAsync();

        // The catch handler swallowed the persist failure (see L114-119
        // of ReplicationMaintenanceGrain). The in-memory LastGcTicks
        // must therefore reflect the pre-mutation value (0) so the
        // next phase tick's ShouldRunCadence guard correctly fires
        // the retry promised by the surrounding comment.
        Assert.That(state.State.LastGcTicks, Is.EqualTo(0L));
    }

    [Test]
    public async Task ProcessNextPhase_GC_persist_failure_does_not_skip_next_phase_tick_retry()
    {
        var sentinel = DateTime.UtcNow.Ticks;
        var seed = new ReplicationMaintenanceState
        {
            LastOrphanSweepTicks = sentinel,
            LastFallOffCheckTicks = sentinel,
        };
        var (grain, state, _, gc, _, _, _, _) = Create(seed: seed);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await grain.ProcessNextPhaseAsync();
        await grain.ProcessNextPhaseAsync();

        // The first tick's persist threw; the second tick must retry
        // (per the XML doc's "thrown GC retries on the next phase tick"
        // contract). Pre-fix, the in-memory LastGcTicks was left at
        // nowTicks after the throw, so the second tick's
        // ShouldRunCadence guard returned false and gc.RunOnceAsync
        // was only called once.
        await gc.Received(2).RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ProcessNextPhase_reverts_LastOrphanSweepTicks_when_WriteStateAsync_throws()
    {
        // Seed the GC and fall-off stamps to skip those blocks. The
        // orphan-sweep block (L130-142) has no try/catch wrapping its
        // WriteStateAsync, so the throw propagates out of
        // ProcessNextPhaseAsync. The snapshot/restore must run before
        // the rethrow so the in-memory stamp does not latch.
        var sentinel = DateTime.UtcNow.Ticks;
        var seed = new ReplicationMaintenanceState
        {
            LastGcTicks = sentinel,
            LastFallOffCheckTicks = sentinel,
        };
        var (grain, state, _, _, _, _, _, _) = Create(seed: seed);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.ProcessNextPhaseAsync());

        Assert.That(state.State.LastOrphanSweepTicks, Is.EqualTo(0L));
    }

    [Test]
    public async Task ProcessNextPhase_reverts_LastFallOffCheckTicks_when_WriteStateAsync_throws()
    {
        // Seed the GC and orphan-sweep stamps to skip those blocks.
        // The fall-off probe block's catch handler swallows the
        // persist failure (L155-160), so ProcessNextPhaseAsync returns
        // normally and we assert directly on the state field.
        var sentinel = DateTime.UtcNow.Ticks;
        var seed = new ReplicationMaintenanceState
        {
            LastGcTicks = sentinel,
            LastOrphanSweepTicks = sentinel,
        };
        var (grain, state, _, _, _, _, _, _) = Create(seed: seed);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.LastFallOffCheckTicks, Is.EqualTo(0L));
    }
}