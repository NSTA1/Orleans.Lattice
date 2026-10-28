using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

[TestFixture]
public sealed class DataValueRenderingTests
{
    [Test]
    public void Render_members_without_provenance_do_not_invent_a_replica_or_ordinal()
    {
        DataCrdtMember[] members =
        [
            new() { ElementText = "first\r\nsecond", ElementFormat = ValueFormat.Text, ReplicaId = "", Ordinal = 99 },
            new() { ElementText = "<b>value</b>", ElementFormat = ValueFormat.Text, ReplicaId = "replica-a", Ordinal = 1204 },
        ];

        var rendered = DataValueRendering.Render([], truncated: true, DataValueRenderer.Members, members);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.FormatText, Is.EqualTo("CRDT members"));
            Assert.That(rendered.Content, Is.EqualTo("first second\n<b>value</b>  (replica replica-a, #1204)\n"));
            Assert.That(rendered.Note, Is.Null, "Decoded members are not a byte preview.");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Render_members_without_decoded_members_falls_back_to_the_automatic_renderer(bool emptyList)
    {
        var rendered = DataValueRendering.Render(
            "plain text"u8.ToArray(), truncated: false, DataValueRenderer.Members,
            emptyList ? [] : null);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.FormatText, Is.EqualTo("UTF-8 text"));
            Assert.That(rendered.Content, Is.EqualTo("plain text"));
            Assert.That(rendered.Note, Is.Null);
        });
    }

    [Test]
    public void Render_empty_automatic_value_is_labelled_empty()
    {
        var rendered = DataValueRendering.Render([], truncated: false, DataValueRenderer.Auto);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.FormatText, Is.EqualTo("Empty"));
            Assert.That(rendered.Content, Is.Empty);
            Assert.That(rendered.Note, Is.Null);
            Assert.That(DataValueRendering.Inline([], truncated: false), Is.Empty);
        });
    }

    [Test]
    public void Render_automatic_binary_value_is_labelled_hex()
    {
        var rendered = DataValueRendering.Render([0xFF], truncated: false, DataValueRenderer.Auto);

        Assert.Multiple(() =>
        {
            Assert.That(rendered.FormatText, Is.EqualTo("Hex"));
            Assert.That(rendered.Content, Does.StartWith("00000000  ff"));
            Assert.That(rendered.Note, Is.Null);
        });
    }

    [TestCase((int)DataValueRenderer.Auto, "Automatic")]
    [TestCase((int)DataValueRenderer.Text, "UTF-8 text")]
    [TestCase((int)DataValueRenderer.Json, "JSON")]
    [TestCase((int)DataValueRenderer.Hex, "Hex")]
    [TestCase((int)DataValueRenderer.Members, "CRDT members")]
    [TestCase(999, "Automatic")]
    public void Label_every_choice_and_an_unknown_choice_has_a_readable_name(int renderer, string expected) =>
        Assert.That(DataValueRendering.Label((DataValueRenderer)renderer), Is.EqualTo(expected));

    [TestCase(false)]
    [TestCase(true)]
    public void Render_invalid_json_reports_the_fallback_and_whether_bytes_are_only_a_preview(bool truncated)
    {
        var rendered = DataValueRendering.Render("not json"u8.ToArray(), truncated, DataValueRenderer.Json);
        const string Fallback = "This value is not valid JSON, so it is shown as text.";
        var expectedNote = truncated
            ? "Preview only: the full value is longer than the bytes fetched. " + Fallback
            : Fallback;

        Assert.Multiple(() =>
        {
            Assert.That(rendered.FormatText, Is.EqualTo("UTF-8 text"));
            Assert.That(rendered.Content, Is.EqualTo("not json"));
            Assert.That(rendered.Note, Is.EqualTo(expectedNote));
        });
    }

    [TestCase((int)DataValueRenderer.Auto)]
    [TestCase((int)DataValueRenderer.Text)]
    [TestCase((int)DataValueRenderer.Json)]
    [TestCase((int)DataValueRenderer.Hex)]
    [TestCase((int)DataValueRenderer.Members)]
    public void Render_null_bytes_are_rejected_before_selecting_a_renderer(int renderer) =>
        Assert.That(() => DataValueRendering.Render(null!, false, (DataValueRenderer)renderer), Throws.ArgumentNullException);
}
