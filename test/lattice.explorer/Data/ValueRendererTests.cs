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
    public void Render_truncated_text_cut_inside_a_character_reads_as_text()
    {
        // #4353: the state API cuts a preview at a byte budget, so a preview of
        // two-byte characters can end on the first byte of one.
        var preview = Encoding.UTF8.GetBytes(new string('\u00e9', 300))[..511];

        var rendered = ValueRenderer.Render(preview, truncated: true);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Text));
            Assert.That(rendered.Content, Is.EqualTo(new string('\u00e9', 255)));
            Assert.That(rendered.Note, Is.Not.Null);
        });
    }

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public void Render_truncated_text_drops_only_the_incomplete_last_character(int bytesOfLastCharacter)
    {
        var emoji = Encoding.UTF8.GetBytes("\U0001F600");
        byte[] preview = [.. Encoding.UTF8.GetBytes("ab\U0001F600"), .. emoji[..bytesOfLastCharacter]];

        var rendered = ValueRenderer.Render(preview, truncated: true);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Text));
            Assert.That(rendered.Content, Is.EqualTo("ab\U0001F600"));
        });
    }

    [Test]
    public void Render_whole_value_ending_inside_a_character_is_still_binary()
    {
        // Only a preview is cut at an arbitrary byte; a whole value that ends
        // part-way through a character is not UTF-8 text.
        byte[] bytes = [.. Encoding.UTF8.GetBytes("ab"), 0xC3];

        Assert.That(ValueRenderer.Render(bytes).Format, Is.EqualTo(ValueFormat.Hex));
    }

    [TestCase(new byte[] { 0x61, 0xFF, 0x62, 0xC3 })]
    [TestCase(new byte[] { 0x61, 0xC3, 0x41 })]
    [TestCase(new byte[] { 0x61, 0x80 })]
    public void Render_truncated_preview_with_invalid_bytes_is_still_binary(byte[] preview) =>
        Assert.That(ValueRenderer.Render(preview, truncated: true).Format, Is.EqualTo(ValueFormat.Hex));

    [Test]
    public void Render_json_keeps_non_ascii_and_html_sensitive_characters_as_written()
    {
        // #4325: the default encoder showed this as caf\u00E9 \u003Cb\u003E \u0026 it\u0027s \u002B44 ...
        const string Text = "caf\u00e9 <b> & it's +44 \u4e2d\u6587";
        var bytes = Encoding.UTF8.GetBytes("{\"name\":\"" + Text + "\"}");

        var rendered = ValueRenderer.Render(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Json));
            Assert.That(rendered.Content, Does.Contain("\"name\": \"" + Text + "\""));
            Assert.That(rendered.Content, Does.Not.Contain("\\u"));
        });
    }

    [Test]
    public void Render_json_still_escapes_what_json_requires_and_reads_back_unchanged()
    {
        const string Text = "say \"hi\" \\ then\nnext";
        var bytes = Encoding.UTF8.GetBytes("""{"q":"say \"hi\" \\ then\nnext"}""");

        var rendered = ValueRenderer.Render(bytes);

        using var reparsed = System.Text.Json.JsonDocument.Parse(rendered.Content);
        Assert.Multiple(() =>
        {
            Assert.That(rendered.Format, Is.EqualTo(ValueFormat.Json));
            Assert.That(reparsed.RootElement.GetProperty("q").GetString(), Is.EqualTo(Text));
        });
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
