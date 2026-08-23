using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL cursor registry's
/// per-consumer monotonic merge - the floor every WAL GC pass trims against.
/// They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real
/// <see cref="Orleans.Lattice.InMemoryWalCursorRegistry"/> under reordered cursor
/// reports, and the guard test swaps its max-merge for last-writer-wins so Coyote
/// re-finds a cursor regressing below a reported frontier - a vacuous model that
/// still passed with the guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalCursorMonotonicityCoyoteTests
{
    /// <summary>
    /// The fix: reporting every cursor through the real registry's max-merge
    /// admits no interleaving of reordered / re-delivered reports where a
    /// consumer's stored cursor regresses below its highest reported frontier -
    /// for two consumers and wider - and the min cursor converges to the full
    /// frontier once all consumers report.
    /// </summary>
    [Test]
    public void Max_merge_never_regresses_a_reported_cursor([Values(2, 3, 4)] int consumerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalCursorMonotonicityModel(consumerCount, WalCursorMonotonicityMode.RegistryMaxMerge));
    }

    /// <summary>
    /// The guard: replacing the merge with last-writer-wins lets a re-delivered
    /// stale report pull a consumer's cursor backwards, and Coyote must find the
    /// regression. This proves the registry's max-merge exercised in the safety
    /// test above is load-bearing rather than decorative.
    /// </summary>
    [Test]
    public void Last_writer_wins_regresses_a_cursor_under_reordering([Values(2, 3)] int consumerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalCursorMonotonicityModel(consumerCount, WalCursorMonotonicityMode.LastWriterWinsReplace));
    }
}
