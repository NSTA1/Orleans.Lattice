namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeOperation"/> flags enum: individual
/// members are distinct powers of two and combine / test as flags.
/// </summary>
[TestFixture]
public class LatticeOperationTests
{
    [Test]
    public void None_is_zero()
    {
        Assert.That((int)LatticeOperation.None, Is.EqualTo(0));
    }

    [Test]
    public void Members_have_the_expected_flag_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeOperation.Read, Is.EqualTo(1));
            Assert.That((int)LatticeOperation.Write, Is.EqualTo(2));
            Assert.That((int)LatticeOperation.Delete, Is.EqualTo(4));
            Assert.That((int)LatticeOperation.RangeRead, Is.EqualTo(8));
            Assert.That((int)LatticeOperation.RangeDelete, Is.EqualTo(16));
            Assert.That((int)LatticeOperation.CrdtApply, Is.EqualTo(32));
            Assert.That((int)LatticeOperation.AtomicWrite, Is.EqualTo(64));
            Assert.That((int)LatticeOperation.BulkLoad, Is.EqualTo(128));
            Assert.That((int)LatticeOperation.Admin, Is.EqualTo(256));
            Assert.That((int)LatticeOperation.Backup, Is.EqualTo(512));
            Assert.That((int)LatticeOperation.Restore, Is.EqualTo(1024));
        });
    }

    [Test]
    public void Members_are_distinct_single_bits()
    {
        var members = new[]
        {
            LatticeOperation.Read, LatticeOperation.Write, LatticeOperation.Delete,
            LatticeOperation.RangeRead, LatticeOperation.RangeDelete, LatticeOperation.CrdtApply,
            LatticeOperation.AtomicWrite, LatticeOperation.BulkLoad, LatticeOperation.Admin,
            LatticeOperation.Backup, LatticeOperation.Restore,
        };

        var union = LatticeOperation.None;
        foreach (var member in members)
        {
            // Each member contributes a bit not already present in the union.
            Assert.That(union.HasFlag(member), Is.False, $"{member} overlaps an earlier member.");
            union |= member;
        }
    }

    [Test]
    public void Composite_mask_reports_each_constituent_flag()
    {
        var atomic = LatticeOperation.AtomicWrite | LatticeOperation.Write | LatticeOperation.Delete;

        Assert.Multiple(() =>
        {
            Assert.That(atomic.HasFlag(LatticeOperation.AtomicWrite), Is.True);
            Assert.That(atomic.HasFlag(LatticeOperation.Write), Is.True);
            Assert.That(atomic.HasFlag(LatticeOperation.Delete), Is.True);
            Assert.That(atomic.HasFlag(LatticeOperation.Read), Is.False);
        });
    }

    [Test]
    public void Enum_carries_the_Flags_attribute()
    {
        Assert.That(typeof(LatticeOperation).IsDefined(typeof(FlagsAttribute), inherit: false), Is.True);
    }
}
