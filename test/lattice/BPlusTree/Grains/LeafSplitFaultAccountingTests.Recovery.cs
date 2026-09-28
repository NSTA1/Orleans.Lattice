using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A division completed by the recovery path must be metered, and metered as a
/// recovery rather than as a fresh division (issue #2860).
/// <para>
/// A division whose completion throws leaves its intent durable
/// (<see cref="LeafNodeState.SplitInFlight"/>). The next activation finishes it
/// through <c>CompleteRecoverySplitUnderGateAsync</c> before admitting any
/// write. That path previously recorded nothing on either split counter, so in
/// the restart regime the only split activity in a process - the recoveries -
/// read as a tree nothing had ever tried to divide.
/// </para>
/// </summary>
public sealed partial class LeafSplitFaultAccountingTests
{
    /// <summary>
    /// Drives activation one into a division whose sibling initialisation
    /// throws, so its intent is persisted and left unfinished, and returns a
    /// SECOND activation built over the same persisted state - the model of a
    /// reactivation after the fault. The sibling admits every initialisation
    /// after the first, so the second activation's recovery can complete.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State)>
        ReactivatedOverInterruptedSplitAsync(string treeId)
    {
        var initialisations = 0;
        var fault = new InvalidOperationException("sibling refused initialisation");
        var grainFactory = SplitGrainFactory(
            3,
            _ => Interlocked.Increment(ref initialisations) == 1
                ? Task.FromException(fault)
                : Task.CompletedTask);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ShardIndex = 0;

        var first = await LeafOverAsync(grainFactory, state, maxLeafKeys: 3);
        Exception? surfaced = null;
        try
        {
            await first.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
        }
        catch (Exception ex)
        {
            surfaced = ex;
        }

        Assert.That(
            surfaced, Is.SameAs(fault),
            "precondition: activation one's division must throw after its intent is durable");

        // Established from persisted state, not from any counter: the division
        // is stranded mid-flight, which is exactly what the recovery path finds.
        Assert.That(
            state.State.SplitInFlight, Is.True,
            "precondition: the interrupted division must be persisted as in flight");

        return (await LeafOverAsync(grainFactory, state, maxLeafKeys: 3), state);
    }

    [Test]
    public async Task A_division_completed_by_recovery_is_counted_as_recovered_and_not_as_a_new_split()
    {
        var treeId = $"tree-split-recovery-{Guid.NewGuid():N}";
        var (reactivated, state) = await ReactivatedOverInterruptedSplitAsync(treeId);

        var measurements = new List<Measurement>();
        var splits = new List<long>();
        using (ListenForSplits(splits))
        using (ListenForAttempts(measurements))
        {
            // An overwrite of a key the leaf already holds, so the write itself
            // cannot grow the leaf into a fresh division: the only split work
            // this call can do is the recovery it must run before admitting it.
            await reactivated.SetAsync("k00000", Encoding.UTF8.GetBytes("v2"));
        }

        // Established without either counter: the recovery really ran to
        // completion, because the persisted in-flight marker is gone.
        Assert.That(
            state.State.SplitInFlight, Is.False,
            "precondition: the reactivated leaf must have completed the stranded division");

        Assert.Multiple(() =>
        {
            // The defect, in one assertion: this was zero before issue #2860.
            Assert.That(
                measurements.Where(m => m.Outcome == "recovered").Sum(m => m.Value),
                Is.EqualTo(1),
                "a division completed by recovery must be counted exactly once as recovered");

            // Distinguishable from a fresh completion.
            Assert.That(
                measurements.Where(m => m.Outcome == "divided").Sum(m => m.Value),
                Is.Zero,
                "a recovered completion must never be counted as a fresh division");

            Assert.That(
                measurements.Where(m => m.Outcome == "faulted").Sum(m => m.Value),
                Is.Zero,
                "a recovery that completed must not register as a fault");

            // And no double count of the initiation: activation one already
            // counted this division when its intent was persisted.
            Assert.That(
                splits.Sum(), Is.Zero,
                "the recovery must not count the division's initiation a second time");
        });
    }

    private static long _recoveredTotal;

    [Test]
    public void The_recovered_emission_shape_allocates_nothing_per_call_with_a_listener_attached()
    {
        // The exact call shape RecordSplitAttempt uses for the recovered arm:
        // the three-tag non-params Counter<long>.Add overload, a cached static
        // outcome pair, and the cached tenant label. A listener is attached so
        // the tags are genuinely delivered rather than short-circuited by an
        // instrument nothing is listening to.
        var treeId = $"tree-split-recovery-alloc-{Guid.NewGuid():N}";
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitAttempts,
            l => l.SetMeasurementEventCallback<long>(static (_, value, _, _) =>
                Interlocked.Add(ref _recoveredTotal, value)));

        var growth = AllocationProbe.Growth(
            _ => treeId,
            static (tree, iterations) =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    LatticeMetrics.LeafSplitAttempts.Add(1,
                        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, tree),
                        LatticeMetrics.LeafSplitRecovered,
                        LatticeTenantLabel.ForTree(tree));
                }
            },
            smallSize: 1_000,
            largeSize: 2_000);

        Assert.Multiple(() =>
        {
            Assert.That(
                Interlocked.Read(ref _recoveredTotal), Is.GreaterThan(0L),
                "precondition: the listener must actually have received the measurements");
            Assert.That(growth, Is.Zero, "the recovered emission must not allocate per call");
        });
    }
}
