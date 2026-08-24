using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Convergence unit tests for <see cref="TenantLwwRegister{T}"/>. Every case
/// drives the merge directly with hand-built clocks, so the last-writer-wins
/// join is proven deterministic, commutative, and idempotent with no reliance on
/// timing or apply order.
/// </summary>
public sealed class TenantLwwRegisterTests
{
    [Test]
    public void Create_holds_the_value_and_stamp()
    {
        var reg = TenantLwwRegister<int>.Create(7, Clock(10), "w1");

        Assert.Multiple(() =>
        {
            Assert.That(reg.Value, Is.EqualTo(7));
            Assert.That(reg.Clock, Is.EqualTo(Clock(10)));
            Assert.That(reg.WriterId, Is.EqualTo("w1"));
        });
    }

    [Test]
    public void Set_with_a_higher_clock_wins()
    {
        var reg = TenantLwwRegister<int>.Create(1, Clock(10), "w1");

        var next = reg.Set(2, Clock(20), "w1");

        Assert.That(next.Value, Is.EqualTo(2));
    }

    [Test]
    public void Set_with_a_lower_clock_is_a_no_op()
    {
        var reg = TenantLwwRegister<int>.Create(1, Clock(20), "w1");

        var next = reg.Set(2, Clock(10), "w1");

        Assert.That(next.Value, Is.EqualTo(1));
    }

    [Test]
    public void Set_with_an_equal_stamp_is_a_no_op()
    {
        var reg = TenantLwwRegister<int>.Create(1, Clock(10), "w1");

        var next = reg.Set(2, Clock(10), "w1");

        Assert.That(next.Value, Is.EqualTo(1));
    }

    [Test]
    public void Set_breaks_a_clock_tie_by_ordinal_writer_id()
    {
        var reg = TenantLwwRegister<int>.Create(1, Clock(10), "w1");

        var higherWriter = reg.Set(2, Clock(10), "w2");
        var lowerWriter = reg.Set(3, Clock(10), "w0");

        Assert.Multiple(() =>
        {
            Assert.That(higherWriter.Value, Is.EqualTo(2), "a higher ordinal writer wins the tie");
            Assert.That(lowerWriter.Value, Is.EqualTo(1), "a lower ordinal writer loses the tie");
        });
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = TenantLwwRegister<int>.Create(1, Clock(10), "w1");
        var b = TenantLwwRegister<int>.Create(2, Clock(20), "w2");

        var ab = TenantLwwRegister<int>.Merge(a, b);
        var ba = TenantLwwRegister<int>.Merge(b, a);

        Assert.That(ab, Is.EqualTo(ba));
        Assert.That(ab.Value, Is.EqualTo(2));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = TenantLwwRegister<int>.Create(1, Clock(10), "w1");
        var b = TenantLwwRegister<int>.Create(2, Clock(20), "w2");
        var c = TenantLwwRegister<int>.Create(3, Clock(30), "w3");

        var left = TenantLwwRegister<int>.Merge(TenantLwwRegister<int>.Merge(a, b), c);
        var right = TenantLwwRegister<int>.Merge(a, TenantLwwRegister<int>.Merge(b, c));

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left.Value, Is.EqualTo(3));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = TenantLwwRegister<int>.Create(1, Clock(10), "w1");

        var merged = TenantLwwRegister<int>.Merge(a, a);

        Assert.That(merged, Is.EqualTo(a));
    }

    [Test]
    public void Merge_breaks_a_clock_tie_by_ordinal_writer_id()
    {
        var a = TenantLwwRegister<int>.Create(1, Clock(10), "w1");
        var b = TenantLwwRegister<int>.Create(2, Clock(10), "w2");

        var merged = TenantLwwRegister<int>.Merge(a, b);

        Assert.That(merged.Value, Is.EqualTo(2));
    }

    [Test]
    public void Merge_treats_a_null_writer_id_as_lowest()
    {
        var nullWriter = TenantLwwRegister<int>.Create(1, Clock(10), writerId: null);
        var namedWriter = TenantLwwRegister<int>.Create(2, Clock(10), "w0");

        var merged = TenantLwwRegister<int>.Merge(nullWriter, namedWriter);

        Assert.That(merged.Value, Is.EqualTo(2), "any named writer outranks the null writer on a clock tie");
    }
}
