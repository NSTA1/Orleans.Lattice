namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public
/// <see cref="LatticeSnapshotReplayBudgetExceededException"/>: both construction
/// overloads and the <see cref="InvalidOperationException"/> inheritance. The
/// exception aborts a snapshot cursor open before any leaf is materialised or any
/// WAL retention pin is taken, so it is a caller-actionable precondition rather
/// than a server fault.
/// </summary>
[TestFixture]
public class LatticeSnapshotReplayBudgetExceededExceptionTests
{
    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeSnapshotReplayBudgetExceededException(
            "projected replay of 12000 entries exceeds MaxSnapshotReplayEntries of 10000");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("MaxSnapshotReplayEntries"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("materialiser lag probe failed");
        var ex = new LatticeSnapshotReplayBudgetExceededException("replay budget exceeded", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("replay budget exceeded"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        Assert.That(
            new LatticeSnapshotReplayBudgetExceededException("m"),
            Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeSnapshotReplayBudgetExceededException).IsSealed, Is.True);
            Assert.That(typeof(LatticeSnapshotReplayBudgetExceededException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Is_distinct_from_the_snapshot_expiry_type()
    {
        Assert.That(
            new LatticeSnapshotReplayBudgetExceededException("m"),
            Is.Not.InstanceOf<LatticeSnapshotExpiredException>(),
            "an over-budget open and an expired snapshot are separately catchable outcomes");
    }
}
