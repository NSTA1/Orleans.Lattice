using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class EntryChangeSignalTests
{
    [Test]
    public void Ctor_carries_key_kind_and_clock()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 1 };

        var signal = new EntryChangeSignal("k", StateChangeKind.Set, hlc);

        Assert.Multiple(() =>
        {
            Assert.That(signal.Key, Is.EqualTo("k"));
            Assert.That(signal.Kind, Is.EqualTo(StateChangeKind.Set));
            Assert.That(signal.Hlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void Equality_is_by_value()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 7, Counter = 0 };
        var a = new EntryChangeSignal("k", StateChangeKind.Delete, hlc);
        var b = new EntryChangeSignal("k", StateChangeKind.Delete, hlc);
        var c = new EntryChangeSignal("k", StateChangeKind.Set, hlc);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}
