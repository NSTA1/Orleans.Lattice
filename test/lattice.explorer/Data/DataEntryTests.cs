using System.Text;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataEntryTests
{
    [Test]
    public void From_MapsCoreFields()
    {
        var record = new EntryRecord
        {
            Key = "k1",
            ValuePreview = Encoding.UTF8.GetBytes("hello"),
            ValueLength = 5,
            Truncated = true,
            Hlc = new HybridLogicalClock { WallClockTicks = 9, Counter = 2 },
            IsTombstone = true,
            ExpiresAtTicks = 42,
            CrdtShape = "OrSet",
        };

        var entry = DataEntry.From(record);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.EqualTo("k1"));
            Assert.That(entry.Value, Is.EqualTo(Encoding.UTF8.GetBytes("hello")));
            Assert.That(entry.ValueLength, Is.EqualTo(5));
            Assert.That(entry.Truncated, Is.True);
            Assert.That(entry.Hlc.Counter, Is.EqualTo(2));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(42));
            Assert.That(entry.CrdtShape, Is.EqualTo("OrSet"));
        });
    }

    [Test]
    public void From_CrdtRecord_MapsCurrentMembers()
    {
        var record = new EntryRecord
        {
            Key = "k1",
            CrdtShape = "OrSet",
            CurrentMembers = new[]
            {
                new CrdtMemberChange
                {
                    Element = Encoding.UTF8.GetBytes("apple"),
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = "eu",
                    Ordinal = 1,
                },
                new CrdtMemberChange
                {
                    Element = Encoding.UTF8.GetBytes("pear"),
                    Kind = CrdtMemberChangeKind.Removed,
                    ReplicaId = "us",
                    Ordinal = 2,
                },
            },
        };

        var entry = DataEntry.From(record);

        Assert.Multiple(() =>
        {
            Assert.That(entry.CurrentMembers, Has.Count.EqualTo(2));
            Assert.That(entry.CurrentMembers[0].ElementText, Is.EqualTo("apple"));
            Assert.That(entry.CurrentMembers[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(entry.CurrentMembers[1].ElementText, Is.EqualTo("pear"));
            Assert.That(entry.CurrentMembers[1].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
        });
    }

    [Test]
    public void From_LwwRecord_HasNoCurrentMembers()
    {
        var record = new EntryRecord
        {
            Key = "k1",
            ValuePreview = Encoding.UTF8.GetBytes("opaque"),
            ValueLength = 6,
            CrdtShape = null,
        };

        var entry = DataEntry.From(record);

        Assert.That(entry.CrdtShape, Is.Null);
        Assert.That(entry.CurrentMembers, Is.Empty);
    }

    [Test]
    public void From_NullRecord_Throws()
    {
        Assert.That(() => DataEntry.From(null!), Throws.TypeOf<ArgumentNullException>());
    }
}
