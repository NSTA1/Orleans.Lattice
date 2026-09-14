using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Arming gate for the starvation-drive verdicts added by issue #2692, built to
/// the standard issue #2938 established for the terminal reactivation outcomes.
/// </summary>
/// <remarks>
/// <para>
/// The defect this exists to prevent has already happened once on this
/// instrument, one group of arms over. Three of the four terminal outcomes
/// shipped with no arm at all, so their occurrences were counted on no series
/// while the instrument's own description promised the reader a measured zero.
/// Every metric gate passed, because a structurally impossible value and a
/// value that genuinely never occurred produce the same scrape.
/// </para>
/// <para>
/// The drive verdicts are a second enum feeding the same instrument, so they
/// inherit that hazard exactly. This gate takes its member list from
/// <see cref="LeafStarvationDriveOutcome"/> itself rather than from a
/// hand-written list, for the reason its sibling gate gives: a gate whose input
/// came from anywhere other than the enum could not detect the case it exists
/// for, since the member that goes missing is precisely the one a hand-written
/// list would also omit.
/// </para>
/// </remarks>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public void DriveOutcomeTag_arms_every_declared_starvation_drive_verdict()
    {
        var members = Enum.GetValues<LeafStarvationDriveOutcome>();

        // Method Rule 2: an empty member list would make every assertion below
        // vacuously true and report a clean gate that scanned nothing.
        Assert.That(members, Has.Length.GreaterThanOrEqualTo(5),
            "the verdict enum must be non-trivial, or this gate passes without checking anything.");

        var armed = members
            .Select(member => LatticeWalGcScheduler.DriveOutcomeTag(member).Value as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(armed, Has.None.Null,
                "every verdict must map to a named arm.");
            Assert.That(armed, Is.Unique,
                "two verdicts sharing an arm would silently sum, which is the folding the arms exist to prevent.");
            Assert.That(armed, Is.EquivalentTo(new[]
                {
                    "drove_lifted", "drove_no_advance", "drove_memory_refused",
                    "drove_not_driven", "drove_already_driving",
                }),
                "the arm set must match what the instrument's description, the docs row and the dashboard panels all claim it is.");
        });
    }

    [Test]
    public void DriveOutcomeTag_throws_rather_than_folding_an_unmapped_verdict()
    {
        // The runtime half of the same guarantee, and the reason the mapping is
        // a throw rather than a catch-all. A member added without being mapped
        // would otherwise take the fallback arm and produce a plausible wrong
        // number in exactly the place a reader trusts one - strictly worse than
        // a crash, because nothing anywhere would say the number had moved.
        //
        // The undeclared value is cast rather than declared, so this test does
        // not itself widen the enum it is guarding.
        var undeclared = (LeafStarvationDriveOutcome)int.MaxValue;

        Assert.That(() => LatticeWalGcScheduler.DriveOutcomeTag(undeclared),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "an unclassified verdict must fail loudly rather than join a neighbouring bucket.");
    }
}
