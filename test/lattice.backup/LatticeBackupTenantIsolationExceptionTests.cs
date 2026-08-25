using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupTenantIsolationException"/>: the
/// exception the tenant scope throws when a capture or restore would cross the
/// active tenant's isolation boundary. It derives directly from
/// <see cref="InvalidOperationException"/> (so it needs no Orleans deep-copier)
/// and carries the supplied message and optional inner exception.
/// </summary>
[TestFixture]
public sealed class LatticeBackupTenantIsolationExceptionTests
{
    [Test]
    public void Message_constructor_sets_the_message()
    {
        var ex = new LatticeBackupTenantIsolationException("boundary crossed");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("boundary crossed"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_and_inner_constructor_sets_both()
    {
        var inner = new InvalidOperationException("cause");
        var ex = new LatticeBackupTenantIsolationException("boundary crossed", inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("boundary crossed"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Derives_directly_from_InvalidOperationException()
    {
        Assert.That(
            typeof(LatticeBackupTenantIsolationException).BaseType,
            Is.EqualTo(typeof(InvalidOperationException)));
    }
}
