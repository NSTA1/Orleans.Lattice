namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of the <see cref="OperatorReseedDecision"/>
/// diagnostic record struct returned by
/// <see cref="ILatticeReplicationAdmin.RequestSnapshotAsync"/>.
/// </summary>
[TestFixture]
public class OperatorReseedDecisionTests
{
    [Test]
    public void Default_value_has_all_fields_zeroed()
    {
        var decision = default(OperatorReseedDecision);
        Assert.Multiple(() =>
        {
            Assert.That(decision.Triggered, Is.False);
            Assert.That(decision.LastRequestedAt, Is.Null);
            Assert.That(decision.RetryAfter, Is.Null);
        });
    }

    [Test]
    public void Constructor_assigns_positional_arguments_in_order()
    {
        var lastAt = DateTimeOffset.UtcNow;
        var retry = TimeSpan.FromSeconds(7);
        var decision = new OperatorReseedDecision(true, lastAt, retry);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Triggered, Is.True);
            Assert.That(decision.LastRequestedAt, Is.EqualTo(lastAt));
            Assert.That(decision.RetryAfter, Is.EqualTo(retry));
        });
    }

    [Test]
    public void Two_decisions_with_identical_fields_are_value_equal()
    {
        var lastAt = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var a = new OperatorReseedDecision(false, lastAt, TimeSpan.FromMinutes(1));
        var b = new OperatorReseedDecision(false, lastAt, TimeSpan.FromMinutes(1));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Two_decisions_with_distinct_triggered_flags_are_not_equal()
    {
        var a = new OperatorReseedDecision(true, null, null);
        var b = new OperatorReseedDecision(false, null, null);

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
