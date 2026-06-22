using System.Text;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class ValueRendererTests
{
    [Test]
    public void Render_EmptyBytes_IsEmptyFormat()
    {
        var rendered = ValueRenderer.Render(Array.Empty<byte>());

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Empty));
            Assert.That(rendered.Content, Is.Empty);
        });
    }

    [Test]
    public void Render_JsonValue_PrettyPrints()
    {
        var bytes = Encoding.UTF8.GetBytes("{\"a\":1,\"b\":[2,3]}");

        var rendered = ValueRenderer.Render(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Json));
            Assert.That(rendered.Content, Does.Contain("\n"));
            Assert.That(rendered.Content, Does.Contain("\"a\": 1"));
        });
    }

    [Test]
    public void Render_PlainText_IsTextFormat()
    {
        var bytes = Encoding.UTF8.GetBytes("hello world");

        var rendered = ValueRenderer.Render(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Text));
            Assert.That(rendered.Content, Is.EqualTo("hello world"));
        });
    }

    [Test]
    public void Render_BinaryValue_IsHexDump()
    {
        var bytes = new byte[] { 0x00, 0x01, 0x02, 0xff, 0xfe };

        var rendered = ValueRenderer.Render(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Hex));
            Assert.That(rendered.Content, Does.StartWith("00000000"));
            Assert.That(rendered.Content, Does.Contain("ff"));
        });
    }

    [Test]
    public void Render_TruncatedValue_SkipsJsonAndCarriesNote()
    {
        // Valid-looking but incomplete JSON; still printable UTF-8.
        var bytes = Encoding.UTF8.GetBytes("{\"a\":1");

        var rendered = ValueRenderer.Render(bytes, truncated: true);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Text));
            Assert.That(rendered.Note, Is.Not.Null);
        });
    }

    [Test]
    public void Render_NonTruncatedValue_HasNoNote()
    {
        var rendered = ValueRenderer.Render(Encoding.UTF8.GetBytes("ok"));

        Assert.That(rendered.Note, Is.Null);
    }

    [Test]
    public void HexDump_FormatsOffsetAndAscii()
    {
        var dump = ValueRenderer.HexDump(Encoding.ASCII.GetBytes("AB"));

        Assert.Multiple(() =>
        {
            Assert.That(dump, Does.StartWith("00000000"));
            Assert.That(dump, Does.Contain("41 42"));
            Assert.That(dump.TrimEnd(), Does.EndWith("AB"));
        });
    }
}
