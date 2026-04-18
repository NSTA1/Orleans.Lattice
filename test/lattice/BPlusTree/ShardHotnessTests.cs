using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

public class ShardHotnessTests
{
    [Test]
    public void Default_has_zero_counters_and_zero_window()
    {
        var hotness = default(ShardHotness);
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(0));
        Assert.That(hotness.Window, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void Init_sets_properties()
    {
        var hotness = new ShardHotness
        {
            Reads = 100,
            Writes = 50,
            Window = TimeSpan.FromSeconds(30),
        };

        Assert.That(hotness.Reads, Is.EqualTo(100));
        Assert.That(hotness.Writes, Is.EqualTo(50));
        Assert.That(hotness.Window, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Record_struct_equality()
    {
        var a = new ShardHotness { Reads = 10, Writes = 5, Window = TimeSpan.FromSeconds(1) };
        var b = new ShardHotness { Reads = 10, Writes = 5, Window = TimeSpan.FromSeconds(1) };
        var c = new ShardHotness { Reads = 10, Writes = 6, Window = TimeSpan.FromSeconds(1) };

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Is.Not.EqualTo(c));
    }
}
