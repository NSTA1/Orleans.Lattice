using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL garbage collector's
/// trim floor - the deterministic, exhaustive counterpart to the stochastic
/// <c>WalTrimUnderShippingChaosTests</c> chaos suite. They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See
/// the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production eligibility rule
/// (<see cref="Orleans.Lattice.WalGcTrimCore.IsEntryEligible"/>) that
/// <c>LatticeWalGc.TrimShardAsync</c> applies to every scanned entry, and the
/// guard test floors under the fastest consumer instead of the slowest so Coyote
/// re-finds the fall-off-the-log data loss - a vacuous model that still passed
/// with the guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalGcTrimFloorCoyoteTests
{
    /// <summary>
    /// The fix: flooring every trim at the minimum consumer cursor admits no
    /// interleaving of concurrent consumer progress and GC passes where the WAL
    /// head is trimmed past the slowest consumer - for two consumers and wider -
    /// and the log is fully reclaimed once every consumer catches up.
    /// </summary>
    [Test]
    public void Min_cursor_floor_never_trims_past_the_slowest_consumer([Values(2, 3, 4)] int consumerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalGcTrimFloorModel(consumerCount, WalGcTrimFloorMode.MinCursorFloor));
    }

    /// <summary>
    /// The guard: flooring under the fastest consumer (dropping a lagging peer
    /// from the min) lets the GC trim entries a slower consumer still needs once
    /// the consumers diverge, and Coyote must find the stranded consumer. This
    /// proves the min-cursor floor in the safety test above is load-bearing
    /// rather than decorative.
    /// </summary>
    [Test]
    public void Max_cursor_floor_strands_a_lagging_consumer([Values(2, 3)] int consumerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalGcTrimFloorModel(consumerCount, WalGcTrimFloorMode.MaxCursorFloorNoLaggard));
    }
}
