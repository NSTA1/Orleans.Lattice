using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Tests for <see cref="RefinementDetectorResolver"/>, the resolver that keeps
/// the Detector column of <c>spec/Refinement.md</c> honest.
/// <para>
/// SELF-REFERENCE IS DELIBERATE. Several of these assert against this fixture's
/// own tests. That is the one anchor in the repository that cannot rot without
/// the assertion rotting with it, so it removes the usual maintenance hazard of
/// a test that hard-codes a name owned by someone else.
/// </para>
/// </summary>
[TestFixture]
internal sealed class RefinementDetectorResolverTests
{
    private static RefinementDetectorResolver Resolver() =>
        RefinementDetectorResolver.ForRepository();

    [Test]
    public void The_resolver_indexes_a_substantial_number_of_tests()
    {
        // Anti-vacuity. A resolver that silently indexed nothing would report
        // every cited detector as missing, which reads as a catastrophic
        // regression rather than as a broken harness. The floor is far below
        // the real count (several thousand) so it cannot become brittle.
        Assert.That(Resolver().IndexedTestCount, Is.GreaterThan(500));
    }

    [Test]
    public void A_test_in_this_fixture_resolves()
    {
        Assert.That(
            Resolver().TestExists(
                nameof(RefinementDetectorResolverTests),
                nameof(A_test_in_this_fixture_resolves)),
            Is.True);
    }

    [Test]
    public void A_test_in_another_fixture_resolves()
    {
        // A detector the refinement note actually cites, so this doubles as a
        // regression on the note's own claim surviving a rename.
        Assert.That(
            Resolver().TestExists(
                "AtomicVisibilityGateTests",
                "Committed_but_already_terminal_orphan_falls_through"),
            Is.True);
    }

    [Test]
    public void A_method_that_carries_no_test_attribute_does_not_resolve()
    {
        // Resolver() is a plain helper in this very fixture. Indexing it would
        // mean the gate accepts any method name at all, which would make the
        // Detector column unfalsifiable in exactly the way #2527 objects to.
        Assert.That(
            Resolver().TestExists(nameof(RefinementDetectorResolverTests), "Resolver"),
            Is.False);
    }

    [Test]
    public void An_absent_test_does_not_resolve()
    {
        Assert.That(
            Resolver().TestExists(
                nameof(RefinementDetectorResolverTests),
                "This_test_does_not_exist_and_must_never_be_added"),
            Is.False);
    }

    [Test]
    public void An_absent_fixture_does_not_resolve()
    {
        Assert.That(Resolver().FixtureExists("NoSuchFixtureAnywhereInThisRepository"), Is.False);
        Assert.That(
            Resolver().TestExists("NoSuchFixtureAnywhereInThisRepository", "Whatever"),
            Is.False);
    }

    [Test]
    public void The_explanation_distinguishes_a_missing_fixture_from_a_missing_test()
    {
        var resolver = Resolver();

        Assert.Multiple(() =>
        {
            Assert.That(
                resolver.Explain("NoSuchFixtureAnywhereInThisRepository", "Whatever"),
                Does.Contain("no fixture named"));

            Assert.That(
                resolver.Explain(
                    nameof(RefinementDetectorResolverTests),
                    "This_test_does_not_exist_and_must_never_be_added"),
                Does.Contain("declares no NUnit-attributed"));

            Assert.That(
                resolver.Explain(
                    nameof(RefinementDetectorResolverTests),
                    nameof(A_test_in_this_fixture_resolves)),
                Does.Contain("resolves"));
        });
    }

    [Test]
    public void The_resolver_validates_its_arguments()
    {
        var resolver = Resolver();

        Assert.Multiple(() =>
        {
            Assert.That(() => new RefinementDetectorResolver(null!), Throws.ArgumentNullException);
            Assert.That(() => resolver.TestExists(null!, "x"), Throws.ArgumentNullException);
            Assert.That(() => resolver.TestExists("x", null!), Throws.ArgumentNullException);
            Assert.That(() => resolver.FixtureExists(null!), Throws.ArgumentNullException);
            Assert.That(() => resolver.Explain(null!, "x"), Throws.ArgumentNullException);
            Assert.That(() => resolver.Explain("x", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_test_declared_after_a_nested_helper_class_still_resolves_to_its_fixture()
    {
        // REGRESSION. The first version of this resolver attributed a method to
        // the nearest preceding type declaration. Both fixtures below declare a
        // private helper class near the top of the file, so every test after it
        // was attributed to the helper and reported as missing - while being
        // present, correctly named, and passing. The gate that consumes this
        // resolver then failed on true statements, which is the one failure
        // mode a staleness gate must not have.
        var resolver = Resolver();

        Assert.Multiple(() =>
        {
            Assert.That(resolver.FixtureExists("ManualTimeProvider"), Is.True, "helper precedes the tests");
            Assert.That(
                resolver.TestExists("TxRegistryGrainTests", "MarkCommittedAsync_throws_when_previously_aborted"),
                Is.True,
                resolver.Explain("TxRegistryGrainTests", "MarkCommittedAsync_throws_when_previously_aborted"));

            Assert.That(resolver.FixtureExists("Harness"), Is.True, "helper precedes the tests");
            Assert.That(
                resolver.TestExists("ShardRootGrainTxTerminalTests", "AppendTxTerminalAsync_fans_out_terminal_to_every_leaf"),
                Is.True,
                resolver.Explain("ShardRootGrainTxTerminalTests", "AppendTxTerminalAsync_fans_out_terminal_to_every_leaf"));

            // The converse: a helper's own members are not promoted to the
            // enclosing fixture, so scoping is real and not merely permissive.
            Assert.That(
                resolver.TestExists("TxRegistryGrainTests", "ManualTimeProvider"),
                Is.False);
        });
    }
}
