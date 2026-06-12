using NUnit.Framework;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="MerkleWalkOutcome"/>.</summary>
[TestFixture]
public sealed class MerkleWalkOutcomeTests
{
    [Test]
    public void NotLocalised_is_not_localised_and_did_not_abort()
    {
        var outcome = MerkleWalkOutcome.NotLocalised;

        Assert.That(outcome.Localised, Is.False);
        Assert.That(outcome.LeavesLocalised, Is.Zero);
        Assert.That(outcome.DepthReached, Is.Zero);
        Assert.That(outcome.AbortReason, Is.EqualTo(MerkleWalkAbortReason.None));
        Assert.That(outcome.BytesInspected, Is.Zero);
    }

    [Test]
    public void Properties_round_trip_through_init()
    {
        var outcome = new MerkleWalkOutcome
        {
            Localised = true,
            LeavesLocalised = 2,
            DepthReached = 3,
            AbortReason = MerkleWalkAbortReason.None,
            BytesInspected = 96,
        };

        Assert.That(outcome.Localised, Is.True);
        Assert.That(outcome.LeavesLocalised, Is.EqualTo(2));
        Assert.That(outcome.DepthReached, Is.EqualTo(3));
        Assert.That(outcome.BytesInspected, Is.EqualTo(96));
    }
}
