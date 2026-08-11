namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class BoundedRegisterTests
{
    private static byte[] Key(params byte[] bytes) => bytes;

    private static BoundedRegister Max(byte[] value, byte[] orderKey)
    {
        var r = BoundedRegister.CreateEmpty(isMin: false);
        r.Set(value, orderKey);
        return r;
    }

    private static BoundedRegister Min(byte[] value, byte[] orderKey)
    {
        var r = BoundedRegister.CreateEmpty(isMin: true);
        r.Set(value, orderKey);
        return r;
    }

    [Test]
    public void New_register_is_bottom()
    {
        var max = BoundedRegister.CreateEmpty(isMin: false);
        var min = BoundedRegister.CreateEmpty(isMin: true);
        Assert.Multiple(() =>
        {
            Assert.That(max.IsBottom, Is.True);
            Assert.That(max.HasValue, Is.False);
            Assert.That(max.IsMin, Is.False);
            Assert.That(min.IsBottom, Is.True);
            Assert.That(min.IsMin, Is.True);
        });
    }

    [Test]
    public void Set_on_empty_register_writes_and_returns_true()
    {
        var r = BoundedRegister.CreateEmpty(isMin: false);
        var advanced = r.Set(Key(0x05), Key(0x05));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(r.HasValue, Is.True);
            Assert.That(r.IsBottom, Is.False);
            Assert.That(r.Value, Is.EqualTo(new byte[] { 0x05 }));
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x05 }));
        });
    }

    [Test]
    public void Set_throws_on_null_value_or_key()
    {
        var r = BoundedRegister.CreateEmpty(isMin: false);
        Assert.Multiple(() =>
        {
            Assert.That(() => r.Set(null!, Key(1)), Throws.ArgumentNullException);
            Assert.That(() => r.Set(Key(1), null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Max_set_forward_advances_register()
    {
        var r = Max(Key(0x01), Key(0x01));
        var advanced = r.Set(Key(0x09), Key(0x09));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
        });
    }

    [Test]
    public void Max_set_backwards_is_a_no_op()
    {
        var r = Max(Key(0x09), Key(0x09));
        var advanced = r.Set(Key(0x01), Key(0x01));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
        });
    }

    [Test]
    public void Min_set_backwards_lower_advances_register()
    {
        var r = Min(Key(0x09), Key(0x09));
        var advanced = r.Set(Key(0x01), Key(0x01));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x01 }));
        });
    }

    [Test]
    public void Min_set_higher_is_a_no_op()
    {
        var r = Min(Key(0x01), Key(0x01));
        var advanced = r.Set(Key(0x09), Key(0x09));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x01 }));
        });
    }

    [Test]
    public void Set_equal_order_key_is_a_no_op_when_value_equal()
    {
        var r = Max(Key(0x05), Key(0x05));
        var advanced = r.Set(Key(0x05), Key(0x05));
        Assert.That(advanced, Is.False);
    }

    [Test]
    public void Order_key_ranks_above_value_bytes()
    {
        // Higher value byte but lower order key must lose for a max register.
        var r = Max(Key(0x01), Key(0x09));
        var advanced = r.Set(Key(0xFF), Key(0x01));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(r.Value, Is.EqualTo(new byte[] { 0x01 }));
        });
    }

    [Test]
    public void Order_key_uses_unsigned_lexicographic_comparison()
    {
        // 0x80 > 0x7F unsigned; a signed comparison would treat 0x80 as negative.
        var r = Max(Key(0x7F), Key(0x7F));
        var advanced = r.Set(Key(0x80), Key(0x80));
        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x80 }));
        });
    }

    [Test]
    public void Max_merge_is_commutative()
    {
        var a = Max(Key(0x03), Key(0x03));
        var b = Max(Key(0x07), Key(0x07));
        var ab = BoundedRegister.Merge(a, b);
        var ba = BoundedRegister.Merge(b, a);
        Assert.Multiple(() =>
        {
            Assert.That(ab.OrderKey, Is.EqualTo(ba.OrderKey));
            Assert.That(ab.Value, Is.EqualTo(ba.Value));
            Assert.That(ab.OrderKey, Is.EqualTo(new byte[] { 0x07 }));
        });
    }

    [Test]
    public void Min_merge_is_commutative()
    {
        var a = Min(Key(0x03), Key(0x03));
        var b = Min(Key(0x07), Key(0x07));
        var ab = BoundedRegister.Merge(a, b);
        var ba = BoundedRegister.Merge(b, a);
        Assert.Multiple(() =>
        {
            Assert.That(ab.OrderKey, Is.EqualTo(ba.OrderKey));
            Assert.That(ab.OrderKey, Is.EqualTo(new byte[] { 0x03 }));
        });
    }

    [Test]
    public void Max_merge_is_associative()
    {
        var a = Max(Key(0x03), Key(0x03));
        var b = Max(Key(0x07), Key(0x07));
        var c = Max(Key(0x05), Key(0x05));
        var left = BoundedRegister.Merge(BoundedRegister.Merge(a, b), c);
        var right = BoundedRegister.Merge(a, BoundedRegister.Merge(b, c));
        Assert.Multiple(() =>
        {
            Assert.That(left.OrderKey, Is.EqualTo(right.OrderKey));
            Assert.That(left.OrderKey, Is.EqualTo(new byte[] { 0x07 }));
        });
    }

    [Test]
    public void Min_merge_is_associative()
    {
        var a = Min(Key(0x03), Key(0x03));
        var b = Min(Key(0x07), Key(0x07));
        var c = Min(Key(0x05), Key(0x05));
        var left = BoundedRegister.Merge(BoundedRegister.Merge(a, b), c);
        var right = BoundedRegister.Merge(a, BoundedRegister.Merge(b, c));
        Assert.Multiple(() =>
        {
            Assert.That(left.OrderKey, Is.EqualTo(right.OrderKey));
            Assert.That(left.OrderKey, Is.EqualTo(new byte[] { 0x03 }));
        });
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = Max(Key(0x07), Key(0x07));
        var merged = BoundedRegister.Merge(a, a);
        Assert.Multiple(() =>
        {
            Assert.That(merged.OrderKey, Is.EqualTo(new byte[] { 0x07 }));
            Assert.That(merged.Value, Is.EqualTo(new byte[] { 0x07 }));
        });
    }

    [Test]
    public void Merge_throws_on_null_operand()
    {
        var a = Max(Key(1), Key(1));
        Assert.Multiple(() =>
        {
            Assert.That(() => BoundedRegister.Merge(null!, a), Throws.ArgumentNullException);
            Assert.That(() => BoundedRegister.Merge(a, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Merge_does_not_mutate_operands()
    {
        var a = Max(Key(0x03), Key(0x03));
        var b = Max(Key(0x07), Key(0x07));
        _ = BoundedRegister.Merge(a, b);
        Assert.Multiple(() =>
        {
            Assert.That(a.OrderKey, Is.EqualTo(new byte[] { 0x03 }));
            Assert.That(b.OrderKey, Is.EqualTo(new byte[] { 0x07 }));
        });
    }

    [Test]
    public void Max_concurrent_writes_converge_to_the_greatest()
    {
        // Two replicas write concurrently; after bidirectional merge both agree
        // on the greatest value regardless of merge order.
        var a = Max(Key(0x02), Key(0x02));
        var b = Max(Key(0x08), Key(0x08));

        var ab = BoundedRegister.Merge(a, b);
        var ba = BoundedRegister.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.OrderKey, Is.EqualTo(new byte[] { 0x08 }));
            Assert.That(ba.OrderKey, Is.EqualTo(new byte[] { 0x08 }));
        });
    }

    [Test]
    public void Min_concurrent_writes_converge_to_the_least()
    {
        var a = Min(Key(0x02), Key(0x02));
        var b = Min(Key(0x08), Key(0x08));

        var ab = BoundedRegister.Merge(a, b);
        var ba = BoundedRegister.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.OrderKey, Is.EqualTo(new byte[] { 0x02 }));
            Assert.That(ba.OrderKey, Is.EqualTo(new byte[] { 0x02 }));
        });
    }

    [Test]
    public void MergeFrom_with_bottom_operand_is_a_no_op()
    {
        var r = Max(Key(0x05), Key(0x05));
        r.MergeFrom(BoundedRegister.CreateEmpty(isMin: false));
        Assert.Multiple(() =>
        {
            Assert.That(r.HasValue, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x05 }));
        });
    }

    [Test]
    public void MergeFrom_into_bottom_adopts_other()
    {
        var r = BoundedRegister.CreateEmpty(isMin: false);
        r.MergeFrom(Max(Key(0x05), Key(0x05)));
        Assert.Multiple(() =>
        {
            Assert.That(r.HasValue, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x05 }));
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        var r = Max(Key(1), Key(1));
        Assert.That(() => r.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MergeDelta_advances_on_winning_candidate()
    {
        var r = Max(Key(0x02), Key(0x02));
        r.MergeDelta(new BoundedRegisterDelta { Value = Key(0x09), OrderKey = Key(0x09), HasValue = true });
        Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
    }

    [Test]
    public void MergeDelta_ignores_losing_candidate()
    {
        var r = Max(Key(0x09), Key(0x09));
        r.MergeDelta(new BoundedRegisterDelta { Value = Key(0x02), OrderKey = Key(0x02), HasValue = true });
        Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
    }

    [Test]
    public void MergeDelta_no_op_delta_is_ignored()
    {
        var r = Max(Key(0x05), Key(0x05));
        r.MergeDelta(BoundedRegisterDelta.Empty);
        r.MergeDelta(default);
        Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x05 }));
    }

    [Test]
    public void MergeDelta_into_bottom_adopts_candidate()
    {
        var r = BoundedRegister.CreateEmpty(isMin: true);
        r.MergeDelta(new BoundedRegisterDelta { Value = Key(0x04), OrderKey = Key(0x04), HasValue = true });
        Assert.Multiple(() =>
        {
            Assert.That(r.HasValue, Is.True);
            Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x04 }));
        });
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var r = Max(Key(0x02), Key(0x02));
        var delta = new BoundedRegisterDelta { Value = Key(0x09), OrderKey = Key(0x09), HasValue = true };
        r.MergeDelta(delta);
        r.MergeDelta(delta);
        Assert.That(r.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
    }

    [Test]
    public void Clone_is_independent_of_source()
    {
        var a = Max(Key(0x03), Key(0x03));
        var clone = a.Clone();
        a.Set(Key(0x09), Key(0x09));
        Assert.Multiple(() =>
        {
            Assert.That(clone.OrderKey, Is.EqualTo(new byte[] { 0x03 }));
            Assert.That(clone.IsMin, Is.False);
            Assert.That(a.OrderKey, Is.EqualTo(new byte[] { 0x09 }));
        });
    }

    [Test]
    public void Clone_preserves_direction()
    {
        var min = Min(Key(0x05), Key(0x05));
        var clone = min.Clone();
        Assert.That(clone.IsMin, Is.True);
    }

    [Test]
    public void Bottom_register_survives_clone()
    {
        var clone = BoundedRegister.CreateEmpty(isMin: true).Clone();
        Assert.Multiple(() =>
        {
            Assert.That(clone.IsBottom, Is.True);
            Assert.That(clone.IsMin, Is.True);
        });
    }

    [Test]
    public void Written_empty_value_is_not_bottom()
    {
        var r = BoundedRegister.CreateEmpty(isMin: false);
        r.Set(Array.Empty<byte>(), Key(0x01));
        Assert.Multiple(() =>
        {
            Assert.That(r.IsBottom, Is.False);
            Assert.That(r.HasValue, Is.True);
            Assert.That(r.Value, Is.EqualTo(Array.Empty<byte>()));
        });
    }
}
