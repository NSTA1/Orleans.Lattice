using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the atomic-commit read gate.
/// They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class AtomicCommitVisibilityCoyoteTests
{
    /// <summary>
    /// The fix: resolving every key of a multi-key read against a single
    /// registry snapshot admits no schedule that produces a split view.
    /// </summary>
    [Test]
    public void Snapshot_gate_never_produces_a_split_view()
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicCommitVisibilityModel(useSnapshot: true));
    }

    /// <summary>
    /// The guard: removing the shared snapshot (a live per-key registry read)
    /// reintroduces the #1584 split-view race, and Coyote must find it. This
    /// proves the model genuinely exercises the race, so the passing test above
    /// is meaningful rather than vacuous.
    /// </summary>
    [Test]
    public void Live_per_key_read_reintroduces_the_split_view_race()
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitVisibilityModel(useSnapshot: false));
    }
}
