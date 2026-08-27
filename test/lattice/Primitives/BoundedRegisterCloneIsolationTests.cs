using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression for the <c>ICrdt.Clone</c> isolation contract on
/// <see cref="BoundedRegister"/>.
/// <para>
/// <c>Clone</c> copied the <c>Value</c> and <c>OrderKey</c> byte-array
/// <em>references</em>, on the documented reasoning that the arrays are treated
/// as immutable inside the type. That reasoning holds inside the type but not at
/// its boundary: a caller that reads a register out of an
/// <c>OrMap&lt;string, BoundedRegister&gt;</c> (the shape the repocontext MCP
/// packages persist) receives a clone whose arrays alias the map's durable
/// state, so writing through the returned <c>Value</c> corrupts the stored CRDT
/// without going through any mutation API.
/// </para>
/// </summary>
[TestFixture]
public class BoundedRegisterCloneIsolationTests
{
    private static BoundedRegister RegisterWith(byte[] value, byte[] orderKey)
    {
        var register = new BoundedRegister();
        register.Set(value, orderKey);
        return register;
    }

    [Test]
    public void Clone_returns_a_register_whose_value_bytes_are_isolated_from_the_source()
    {
        var source = RegisterWith([1, 2, 3], [10]);

        var clone = source.Clone();
        clone.Value![0] = 0x99;

        Assert.That(source.Value![0], Is.EqualTo(1),
            "Clone must return a register that is independent of the receiver");
    }

    [Test]
    public void Clone_returns_a_register_whose_order_key_bytes_are_isolated_from_the_source()
    {
        var source = RegisterWith([1], [10, 20]);

        var clone = source.Clone();
        clone.OrderKey![0] = 0x99;

        Assert.That(source.OrderKey![0], Is.EqualTo(10),
            "the order key decides every subsequent merge and must not be shared across clones");
    }

    [Test]
    public void Clone_of_an_empty_register_stays_empty()
    {
        var clone = new BoundedRegister().Clone();

        Assert.Multiple(() =>
        {
            Assert.That(clone.HasValue, Is.False);
            Assert.That(clone.Value, Is.Null);
            Assert.That(clone.OrderKey, Is.Null);
        });
    }

    [Test]
    public void Clone_preserves_the_min_direction()
    {
        var source = new BoundedRegister { IsMin = true };
        source.Set([5], [5]);

        var clone = source.Clone();
        clone.Set([9], [9]);

        Assert.Multiple(() =>
        {
            Assert.That(clone.IsMin, Is.True);
            Assert.That(clone.Value, Is.EqualTo(new byte[] { 5 }), "a Min register must reject a greater candidate");
        });
    }

    [Test]
    public void Get_on_a_map_of_registers_returns_bytes_isolated_from_the_stored_state()
    {
        var map = new OrMap<string, BoundedRegister>();
        map.Set("k", "r1", RegisterWith([1, 2, 3], [10]));

        map.Get("k")!.Value![0] = 0x99;

        Assert.That(map.Get("k")!.Value![0], Is.EqualTo(1),
            "a read through OrMap.Get must not hand the caller a live handle on the map's durable bytes");
    }

    [Test]
    public void Merge_leaves_the_right_operand_bytes_isolated_from_the_result()
    {
        var left = RegisterWith([1], [10]);
        var right = RegisterWith([2], [20]);

        var merged = BoundedRegister.Merge(left, right);
        merged.Value![0] = 0x99;

        Assert.That(right.Value![0], Is.EqualTo(2),
            "Merge must be pure in both operands");
    }
}
