using System;
using System.Linq;
using NUnit.Framework;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Guards the repair for issue #2970: the cross-file tag-domain resolver used to
/// mint a tag domain out of formatting templates belonging to unrelated types,
/// and reported it in exactly the shape of a successfully resolved closed
/// enumeration.
/// </summary>
/// <remarks>
/// <para>
/// The mechanism was an unqualified method-name descent. The grain_type value at
/// <c>LatticeGrainCallObservationFilter.cs:225</c> is <c>t.ToString()</c>; the
/// resolver could not bind it, fell through to its call-descent branch, asked for
/// every method under <c>src/</c> named <c>ToString</c>, and harvested every
/// string literal in every one of them. The published domain was seven
/// <c>ToString</c> templates from types with no connection to the instrument.
/// </para>
/// <para>
/// <b>Why the fix is not the one the issue proposed.</b> Issue #2970 proposes
/// excluding brace-delimited placeholder text. Measured against the real output,
/// three of the seven fabricated values carried no brace at all
/// (<c>anonymous</c>, <c>token(username=</c>, and <c>, secret=redacted)</c>), so
/// that remedy would have removed four and published the remaining three as a
/// tidy three-value enumeration - less obviously wrong while still entirely
/// wrong, and containing a value indistinguishable from a legitimate tag. The
/// defect is name-collision descent, not placeholder syntax, so the repair is at
/// the descent.
/// </para>
/// <para>
/// <b>The deliberate asymmetry.</b> Refusing an ambiguous descent costs a domain
/// this resolver then reports unresolved. Taking one produces a confident wrong
/// answer that every downstream gate believes. Those costs are not symmetric, so
/// the guard is not either.
/// </para>
/// <para>
/// <b>What is deliberately not tested here.</b> No assertion pins the arity of
/// any instrument's domain to a literal number. The arity of
/// <c>wal.gc.blocked_leaf_reactivations</c> moved from eight to thirteen when
/// issue #2692 added five drive verdicts, and typing a number into a gate built
/// to catch stale numbers is how the ninth mismatch gets authored. The positive
/// control below therefore asserts that a domain resolves and is not reported
/// unresolved, never that it has a particular size.
/// </para>
/// </remarks>
[TestFixture]
public class TagDomainUnresolvedTests
{
    /// <summary>
    /// The two instruments whose grain_type domain was fabricated, named by their
    /// canonical dotted names.
    /// </summary>
    private const string GrainCallDuration = "orleans.lattice.grain.call.duration";
    private const string GrainCallOutstandingDepth = "orleans.lattice.grain.call.outstanding_depth";

    /// <summary>
    /// An instrument whose outcome domain the resolver reads correctly, used as
    /// the positive control throughout.
    /// </summary>
    private const string BlockedLeafReactivations =
        "orleans.lattice.wal.gc.blocked_leaf_reactivations";

    [TestCase(GrainCallDuration)]
    [TestCase(GrainCallOutstandingDepth)]
    public void Grain_type_no_longer_resolves_to_a_fabricated_domain(string instrument)
    {
        Assert.That(
            DashboardPanelTagDomainTests.CoversInstrument(instrument),
            Is.True,
            $"{instrument} is not in the resolver's scope, so this test would pass without "
            + "observing anything. The instrument was renamed or its owner type left the "
            + "resolver's enumeration; repoint the test rather than deleting it.");

        var armed = DashboardPanelTagDomainTests.ArmedValues(instrument, "grain_type");

        Assert.That(
            armed,
            Is.Null,
            "grain_type is a grain type name supplied at the call site, so its domain is open "
            + "and no bounded enumeration for it can be correct. Before issue #2970 this "
            + "resolved to seven ToString templates harvested from unrelated types.");
    }

    [TestCase(GrainCallDuration)]
    [TestCase(GrainCallOutstandingDepth)]
    public void Grain_type_is_reported_unresolved_with_a_reason_naming_the_ambiguity(string instrument)
    {
        var reason = DashboardPanelTagDomainTests.UnresolvedReason(instrument, "grain_type");

        Assert.That(
            reason,
            Is.Not.Null,
            "An undecidable domain must say so. Returning no values without a reason is the "
            + "defect issue #2968 records against the other tag parser: a parser failure "
            + "presented in the same shape as a measured absence.");

        Assert.That(
            reason,
            Does.Contain("ambiguous method-name descent"),
            $"The reason must name the mechanism that stopped the resolution, not merely that "
            + $"one did. Actual reason: '{reason}'.");
    }

    /// <summary>
    /// Positive control. The guard must not have been bought by making the
    /// resolver refuse everything.
    /// </summary>
    [Test]
    public void A_resolvable_domain_is_still_resolved_and_not_reported_unresolved()
    {
        Assert.That(
            DashboardPanelTagDomainTests.CoversInstrument(BlockedLeafReactivations),
            Is.True,
            $"{BlockedLeafReactivations} left the resolver's scope, so this control observes "
            + "nothing and the refusal tests above prove only that the resolver is silent.");

        var armed = DashboardPanelTagDomainTests.ArmedValues(BlockedLeafReactivations, "outcome");

        Assert.That(
            armed,
            Is.Not.Null,
            "This domain resolves through a helper parameter, not through an ambiguous method "
            + "name, so the issue #2970 refusal must leave it untouched. If this is null, the "
            + "refusal is too wide and is now suppressing correct answers.");

        Assert.That(
            armed!.Count,
            Is.GreaterThan(1),
            "A single-value domain would mean the resolver degraded to harvesting one literal.");

        // Deliberately one known member rather than the full set: this asserts the
        // harness observes presence, without pinning an arity that legitimately grows.
        Assert.That(
            armed,
            Does.Contain("attempted"),
            "The lifecycle arm 'attempted' is emitted by the WAL GC scheduler and must be "
            + "observable, otherwise this control is not demonstrating presence at all.");

        Assert.That(
            DashboardPanelTagDomainTests.UnresolvedReason(BlockedLeafReactivations, "outcome"),
            Is.Null,
            "A domain that resolved must carry no unresolved reason, or the two states are "
            + "not in fact distinguishable.");
    }

    /// <summary>
    /// Anti-vacuity. The refusal only fires for a method name declared more than
    /// once under <c>src/</c>; if no such name existed the guard would be dead
    /// code and issue #2970 could return without reddening anything.
    /// </summary>
    [Test]
    public void The_ambiguity_condition_is_reachable_so_the_guard_cannot_be_vacuous()
    {
        var toStringDeclarations = DashboardPanelTagDomainTests.DeclarationCount("ToString");

        Assert.That(
            toStringDeclarations,
            Is.GreaterThan(1),
            "The resolver found at most one method named 'ToString' under src/. Either the "
            + "source scan is matching nothing - in which case every domain this resolver "
            + "reports is derived from an empty corpus - or the declaration matcher stopped "
            + "recognising overrides. Both make the issue #2970 guard inert while leaving it "
            + "green, which is the failure shape this whole family of gates exists to remove.");
    }

    /// <summary>
    /// The three states must be separable by a caller. Collapsing "not covered"
    /// into "undecided" is the exact conflation issue #2968 records.
    /// </summary>
    [Test]
    public void Not_covered_undecided_and_resolved_are_three_distinguishable_states()
    {
        const string notCovered = "orleans.lattice.no.such.instrument";

        Assert.That(
            DashboardPanelTagDomainTests.CoversInstrument(notCovered),
            Is.False,
            "The sentinel name must be outside the resolver's scope for this test to mean "
            + "anything.");

        // Not covered: no values, and no reason, because the resolver makes no
        // claim at all about an instrument it does not read.
        Assert.Multiple(() =>
        {
            Assert.That(DashboardPanelTagDomainTests.ArmedValues(notCovered, "outcome"), Is.Null);
            Assert.That(DashboardPanelTagDomainTests.UnresolvedReason(notCovered, "outcome"), Is.Null);

            // Undecided: no values, but a reason.
            Assert.That(
                DashboardPanelTagDomainTests.ArmedValues(GrainCallDuration, "grain_type"),
                Is.Null);
            Assert.That(
                DashboardPanelTagDomainTests.UnresolvedReason(GrainCallDuration, "grain_type"),
                Is.Not.Null);

            // Resolved: values, and no reason.
            Assert.That(
                DashboardPanelTagDomainTests.ArmedValues(BlockedLeafReactivations, "outcome"),
                Is.Not.Null);
            Assert.That(
                DashboardPanelTagDomainTests.UnresolvedReason(BlockedLeafReactivations, "outcome"),
                Is.Null);
        });

        // The pair (values, reason) must therefore take three distinct shapes. A
        // caller that reads only one of the two cannot tell all three apart, which
        // is why both accessors exist.
        Assert.That(
            new[]
            {
                (DashboardPanelTagDomainTests.ArmedValues(notCovered, "outcome") is not null,
                    DashboardPanelTagDomainTests.UnresolvedReason(notCovered, "outcome") is not null),
                (DashboardPanelTagDomainTests.ArmedValues(GrainCallDuration, "grain_type") is not null,
                    DashboardPanelTagDomainTests.UnresolvedReason(GrainCallDuration, "grain_type") is not null),
                (DashboardPanelTagDomainTests.ArmedValues(BlockedLeafReactivations, "outcome") is not null,
                    DashboardPanelTagDomainTests.UnresolvedReason(BlockedLeafReactivations, "outcome") is not null),
            }.Distinct().Count(),
            Is.EqualTo(3),
            "The three states collapsed into fewer than three observable shapes, so a "
            + "downstream audit cannot tell a parser failure from a measured absence.");
    }

    /// <summary>
    /// An unresolved domain must carry an empty value set as well as a reason, so
    /// that this resolver and <c>InstrumentPrimingEnrolment</c> describe the same
    /// state in the same shape (issues #2968, #2970).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The other parser represents "I could not read this" as an empty value list
    /// plus a flag naming what was unread. A cross-gate audit can join the two on
    /// "did either parser admit it could not read this" only while both sides
    /// hold that shape, so this asserts the shape on the live corpus rather than
    /// on a hand-built instance.
    /// </para>
    /// <para>
    /// The invariant itself is enforced by the type, not by this test: the
    /// <c>TagDomain</c> primary constructor is private and its unresolved factory
    /// fixes the value set empty, so "unresolved, and here are some values" is a
    /// build error rather than an assertion a later edit could delete. This test
    /// covers the part a type cannot: that the resolver actually routes the real
    /// grain_type case through that factory, and did not simply stop deriving.
    /// </para>
    /// </remarks>
    [TestCase(GrainCallDuration)]
    [TestCase(GrainCallOutstandingDepth)]
    public void An_unresolved_domain_carries_no_values_so_both_parsers_agree_in_shape(
        string instrument)
    {
        var derived = DashboardPanelTagDomainTests.DerivedValueCount(instrument, "grain_type");

        Assert.That(
            derived,
            Is.Not.EqualTo(-1),
            $"{instrument} is out of the resolver's scope, so this test observes nothing.");

        Assert.Multiple(() =>
        {
            Assert.That(
                derived,
                Is.Zero,
                "An unresolved domain that still carries values would let a consumer reading "
                + "only the value set treat a lower bound as a domain. That is the #2970 "
                + "defect restated in a quieter field.");

            Assert.That(
                DashboardPanelTagDomainTests.UnresolvedReason(instrument, "grain_type"),
                Is.Not.Null,
                "Emptiness alone must never be read as absence, which is why the reason is "
                + "the load-bearing half of the pair and is asserted alongside it here.");
        });
    }
}
