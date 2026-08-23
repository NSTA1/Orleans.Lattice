using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL commit-log writer's
/// shutdown drain releasing parked admission callers. They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See
/// the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production pre-admission gate
/// (<see cref="Orleans.Lattice.BPlusTree.Grains.WalAdmissionGateCore.IsDispatchRefused"/>)
/// and mirrors the token-observing admission wait, and the guard test splits the
/// token check from the park so Coyote re-finds a lost-wakeup that leaves a
/// caller parked after the drain - a vacuous model that still passed with the
/// guard removed would be rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalCommitLogWriterDrainCoyoteTests
{
    /// <summary>
    /// The fix: parking on the admission wait with the drain token in the wait
    /// set admits no interleaving where a caller is left parked after the writer
    /// drains - for one caller and wider - so shutdown always releases every
    /// parked caller in bounded time.
    /// </summary>
    [Test]
    public void Observing_the_drain_token_releases_every_parked_caller([Values(1, 2, 3)] int callerCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalCommitLogWriterDrainModel(callerCount, WalCommitLogWriterDrainMode.ObserveDrainTokenInWait));
    }

    /// <summary>
    /// The guard: sampling the drain token and only then parking lets a caller
    /// that saw the token down park after the drain cancelled it, and Coyote must
    /// find the lost-wakeup wedge. This proves the token-observing wait in the
    /// safety test above is load-bearing rather than decorative.
    /// </summary>
    [Test]
    public void Checking_the_token_before_parking_loses_a_wakeup([Values(1, 2)] int callerCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalCommitLogWriterDrainModel(callerCount, WalCommitLogWriterDrainMode.CheckTokenThenWait));
    }
}
