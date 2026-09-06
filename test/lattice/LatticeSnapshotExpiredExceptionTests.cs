namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeSnapshotExpiredException"/>: both
/// construction overloads and the <see cref="InvalidOperationException"/>
/// inheritance a transport binding relies on to map an expired snapshot read to a
/// client-fault status rather than a server fault.
/// </summary>
[TestFixture]
public class LatticeSnapshotExpiredExceptionTests
{
    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeSnapshotExpiredException("snapshot 'snap-1' has expired");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("snapshot 'snap-1' has expired"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("retention pin was released");
        var ex = new LatticeSnapshotExpiredException("snapshot has expired", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("snapshot has expired"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        Assert.That(new LatticeSnapshotExpiredException("m"), Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeSnapshotExpiredException).IsSealed, Is.True);
            Assert.That(typeof(LatticeSnapshotExpiredException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Is_distinct_from_the_cursor_snapshot_expiry_type()
    {
        Assert.That(
            new LatticeSnapshotExpiredException("m"),
            Is.Not.InstanceOf<LatticeCursorSnapshotExpiredException>(),
            "the snapshot-read and cursor-snapshot expiry faults are separately catchable");
    }
}
