namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeCursorSnapshotExpiredException"/>:
/// both construction overloads and the <see cref="InvalidOperationException"/>
/// inheritance. The exception is raised when a point-in-time cursor outlives the
/// snapshot it was opened against, so the caller must reopen rather than retry.
/// </summary>
[TestFixture]
public class LatticeCursorSnapshotExpiredExceptionTests
{
    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeCursorSnapshotExpiredException("cursor snapshot has expired");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("cursor snapshot has expired"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("pin was reclaimed");
        var ex = new LatticeCursorSnapshotExpiredException("cursor snapshot has expired", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("cursor snapshot has expired"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        Assert.That(
            new LatticeCursorSnapshotExpiredException("m"),
            Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeCursorSnapshotExpiredException).IsSealed, Is.True);
            Assert.That(typeof(LatticeCursorSnapshotExpiredException).IsPublic, Is.True);
        });
    }
}
