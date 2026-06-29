using System.Text;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataCrdtMemberTests
{
    [Test]
    public void From_TextElement_RendersAsText()
    {
        var value = new CrdtMemberValue
        {
            Element = Encoding.UTF8.GetBytes("apple"),
            ReplicaId = "eu",
            Ordinal = 7,
        };

        var member = DataCrdtMember.From(value);

        Assert.Multiple(() =>
        {
            Assert.That(member.ElementText, Is.EqualTo("apple"));
            Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Text));
            Assert.That(member.ReplicaId, Is.EqualTo("eu"));
            Assert.That(member.Ordinal, Is.EqualTo(7));
        });
    }

    [Test]
    public void From_EmptyElement_RendersAsEmptyFormat()
    {
        var value = new CrdtMemberValue
        {
            Element = Array.Empty<byte>(),
            ReplicaId = "eu",
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(value);

        Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Empty));
    }

    [Test]
    public void From_NullElement_TreatedAsEmpty()
    {
        var value = new CrdtMemberValue
        {
            Element = null!,
            ReplicaId = "eu",
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(value);

        Assert.That(member.ElementFormat, Is.EqualTo(ValueFormat.Empty));
    }

    [Test]
    public void From_NullReplicaId_BecomesEmptyString()
    {
        var value = new CrdtMemberValue
        {
            Element = Encoding.UTF8.GetBytes("x"),
            ReplicaId = null!,
            Ordinal = 1,
        };

        var member = DataCrdtMember.From(value);

        Assert.That(member.ReplicaId, Is.EqualTo(string.Empty));
    }
}
