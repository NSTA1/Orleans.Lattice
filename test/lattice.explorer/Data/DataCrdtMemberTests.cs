using System.Text;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataCrdtMemberTests
{
    [Test]
    public void From_TextElement_RendersAsText()
    {
        var change = new CrdtMemberChange
        {
            Element = Encoding.UTF8.GetBytes("apple"),
            Kind = CrdtMemberChangeKind.Added,
            ReplicaId = "eu",
            Ordinal = 7,
        };

        var member = DataCrdtMember.From(change);

        Assert.Multiple(() =>
        {
            Assert.That(member.Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(member.ElementText, Is.EqualTo("apple"));
            Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Text));
            Assert.That(member.ReplicaId, Is.EqualTo("eu"));
            Assert.That(member.Ordinal, Is.EqualTo(7));
        });
    }

    [Test]
    public void From_RemovedKind_IsPreserved()
    {
        var change = new CrdtMemberChange
        {
            Element = Encoding.UTF8.GetBytes("pear"),
            Kind = CrdtMemberChangeKind.Removed,
            ReplicaId = "us",
            Ordinal = 3,
        };

        var member = DataCrdtMember.From(change);

        Assert.That(member.Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
        Assert.That(member.ElementText, Is.EqualTo("pear"));
    }

    [Test]
    public void From_EmptyElement_RendersAsEmptyFormat()
    {
        var change = new CrdtMemberChange
        {
            Element = Array.Empty<byte>(),
            Kind = CrdtMemberChangeKind.Added,
            ReplicaId = "eu",
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(change);

        Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Empty));
    }

    [Test]
    public void From_NullElement_TreatedAsEmpty()
    {
        var change = new CrdtMemberChange
        {
            Element = null!,
            Kind = CrdtMemberChangeKind.Added,
            ReplicaId = "eu",
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(change);

        Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Empty));
    }

    [Test]
    public void From_NullReplicaId_BecomesEmptyString()
    {
        var change = new CrdtMemberChange
        {
            Element = Encoding.UTF8.GetBytes("x"),
            Kind = CrdtMemberChangeKind.Added,
            ReplicaId = null!,
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(change);

        Assert.That(member.ReplicaId, Is.EqualTo(string.Empty));
    }
}
