namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public
/// <see cref="LatticeCursorRegistryPinExhaustedException"/>: both construction
/// overloads and the <see cref="InvalidOperationException"/> inheritance. The
/// exception is the fail-closed outcome when the per-tree cursor registry cannot
/// take another WAL retention pin, so it is caller-actionable (retry or widen the
/// pin budget) rather than a server fault.
/// </summary>
[TestFixture]
public class LatticeCursorRegistryPinExhaustedExceptionTests
{
    [Test]
    public void Message_constructor_preserves_the_message()
    {
        var ex = new LatticeCursorRegistryPinExhaustedException("snapshot pin budget exhausted");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("snapshot pin budget exhausted"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("registry write rejected");
        var ex = new LatticeCursorRegistryPinExhaustedException("pin budget exhausted", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("pin budget exhausted"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_from_InvalidOperationException()
    {
        Assert.That(
            new LatticeCursorRegistryPinExhaustedException("m"),
            Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeCursorRegistryPinExhaustedException).IsSealed, Is.True);
            Assert.That(typeof(LatticeCursorRegistryPinExhaustedException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Is_distinct_from_the_cursor_snapshot_expiry_type()
    {
        Assert.That(
            new LatticeCursorRegistryPinExhaustedException("m"),
            Is.Not.InstanceOf<LatticeCursorSnapshotExpiredException>(),
            "pin exhaustion is retryable whereas an expired cursor snapshot is not, so they stay separately catchable");
    }
}
