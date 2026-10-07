using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for <see cref="RoutingPairPublishModel"/>: a routing
/// activation publishing the pairs it resolves while resolves, registry changes
/// and invalidations interleave, decided by the real <c>RoutingPairPublishGate</c>.
/// Tagged <c>[Category("Coyote")]</c>; see the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class RoutingPairPublishCoyoteTests
{
    /// <summary>
    /// The shipping rule: a published pair never regresses to an older registry
    /// row, and a pair older than an invalidation is never put back.
    /// </summary>
    [Test]
    public void The_published_pair_never_regresses_or_returns_after_an_invalidation()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new RoutingPairPublishModel(RoutingPairPublishGuard.None));
    }

    /// <summary>Without the version check a slow resolve overwrites a newer pair.</summary>
    [Test]
    public void Without_the_version_check_a_slow_resolve_overwrites_a_newer_pair()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(new RoutingPairPublishModel(RoutingPairPublishGuard.NoVersionCheck));
    }

    /// <summary>Specificity: that guard is caught only by the never-regresses assertion.</summary>
    [Test]
    public void Without_the_version_check_only_the_regression_assertion_fires()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new RoutingPairPublishModel(RoutingPairPublishGuard.NoVersionCheck, RoutingPairPublishAssertions.NoStaleRepublish));
    }

    /// <summary>
    /// Without the epoch check a resolve that started before an invalidation puts
    /// back the stale pair the invalidation dropped (#4357's never-healing pair).
    /// </summary>
    [Test]
    public void Without_the_epoch_check_an_invalidated_pair_is_published_again()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(new RoutingPairPublishModel(RoutingPairPublishGuard.NoEpochCheck));
    }

    /// <summary>Specificity: that guard is caught only by the no-stale-republish assertion.</summary>
    [Test]
    public void Without_the_epoch_check_only_the_republish_assertion_fires()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new RoutingPairPublishModel(RoutingPairPublishGuard.NoEpochCheck, RoutingPairPublishAssertions.NeverRegresses));
    }
}
