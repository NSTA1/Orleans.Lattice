using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the N-key atomic-commit read
/// gate. They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class AtomicCommitVisibilityCoyoteTests
{
    /// <summary>
    /// The fix, generalized to N keys: resolving every key of a multi-key read
    /// against a single registry snapshot and double-checking the monotonic
    /// revision admits no schedule that certifies a split view, for both the
    /// original two-key fan-out and a wider one.
    /// </summary>
    [Test]
    public void Snapshot_with_revision_probe_never_certifies_a_split_view([Values(2, 3, 4)] int keyCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicCommitVisibilityModel(keyCount, AtomicCommitReaderMode.SharedSnapshotWithRevisionProbe));
    }

    /// <summary>
    /// The guard: keeping the shared snapshot but removing the revision re-check
    /// lets a commit that drains some leaves mid-fan-out be certified as a torn
    /// read, and Coyote must find it. This proves the probe in the safety test
    /// above is load-bearing rather than decorative.
    /// </summary>
    [Test]
    public void Shared_snapshot_without_revision_probe_certifies_a_torn_read([Values(2, 3)] int keyCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitVisibilityModel(keyCount, AtomicCommitReaderMode.SharedSnapshotWithoutRevisionProbe));
    }

    /// <summary>
    /// The guard preserving the original #1584 behaviour: a live per-key registry
    /// read (no shared snapshot) reintroduces the split-view race, and Coyote must
    /// find it. This keeps the original two-key regression covered as the N=2 case.
    /// </summary>
    [Test]
    public void Live_per_key_read_reintroduces_the_split_view_race([Values(2, 3)] int keyCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitVisibilityModel(keyCount, AtomicCommitReaderMode.LivePerKeyRead));
    }
}
