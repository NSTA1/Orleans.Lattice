using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The <c>lattice-export/1</c> encoding (issue #5366), the contract G1's importer reads
/// back: a value is JSON verbatim exactly when its bytes are one single-line JSON value,
/// otherwise base64 marked as such; NDJSON is one object per line ending in a trailer
/// line; CSV is RFC 4180 with a <c>key,value,encoding</c> header ending in a trailer row.
/// </summary>
[TestFixture]
public sealed class DataExportEncoderTests
{
    [TestCase("{\"a\":1}")]
    [TestCase("[1,2,{\"b\":null}]")]
    [TestCase("\"text\"")]
    [TestCase("42")]
    [TestCase("-1.5e3")]
    [TestCase("true")]
    [TestCase("null")]
    [TestCase("{\"a\": 1, \"b\": [ 2 ]}")]
    [TestCase("\"caf\u00e9\"")]
    public void A_single_line_json_value_is_embeddable(string value) =>
        Assert.That(DataExportEncoder.IsEmbeddableJson(Encoding.UTF8.GetBytes(value)), Is.True);

    [TestCase("")]
    [TestCase(" 1")]
    [TestCase("1 ")]
    [TestCase("{\"a\":\n1}")]
    [TestCase("{\"a\":1}\r")]
    [TestCase("1 2")]
    [TestCase("{\"a\":1,}")]
    [TestCase("/* c */ 1")]
    [TestCase("hello")]
    [TestCase("{\"a\":1")]
    [TestCase("'single'")]
    public void Anything_else_is_not_embeddable(string value) =>
        Assert.That(DataExportEncoder.IsEmbeddableJson(Encoding.UTF8.GetBytes(value)), Is.False);

    [Test]
    public void Invalid_utf8_a_byte_order_mark_and_binary_are_not_embeddable()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataExportEncoder.IsEmbeddableJson([0x22, 0xFF, 0x22]), Is.False, "invalid UTF-8 inside a string");
            Assert.That(DataExportEncoder.IsEmbeddableJson([0xEF, 0xBB, 0xBF, 0x31]), Is.False, "a byte-order mark");
            Assert.That(DataExportEncoder.IsEmbeddableJson([0x00, 0x01, 0x02]), Is.False);
        });
    }

    [Test]
    public void An_ndjson_json_value_is_written_verbatim_and_round_trips_exactly()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);
        var value = Encoding.UTF8.GetBytes("{\"total\": -5, \"status\":\"open\"}");

        var line = Text(encoder.Entry("order/0017", value));

        Assert.That(line, Is.EqualTo("{\"key\":\"order/0017\",\"value\":{\"total\": -5, \"status\":\"open\"}}\n"));
        using var document = JsonDocument.Parse(line);
        Assert.That(Encoding.UTF8.GetBytes(document.RootElement.GetProperty("value").GetRawText()), Is.EqualTo(value));
    }

    [Test]
    public void An_ndjson_non_json_value_is_base64_and_marked()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);
        byte[] value = [0x00, 0xFB, 0xFF, 0x2B];

        var line = Text(encoder.Entry("bin", value));

        Assert.That(line, Is.EqualTo("{\"key\":\"bin\",\"value\":\"APv/Kw==\",\"encoding\":\"base64\"}\n"));
        using var document = JsonDocument.Parse(line);
        Assert.That(document.RootElement.GetProperty("value").GetBytesFromBase64(), Is.EqualTo(value));
    }

    [Test]
    public void An_ndjson_key_with_quotes_control_and_non_ascii_characters_is_valid_json_on_one_line()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);
        const string key = "a \"quoted\"\nkey caf\u00e9";

        var line = Text(encoder.Entry(key, Encoding.UTF8.GetBytes("1")));

        Assert.Multiple(() =>
        {
            Assert.That(line.TrimEnd('\n'), Does.Not.Contain("\n"));
            using var document = JsonDocument.Parse(line);
            Assert.That(document.RootElement.GetProperty("key").GetString(), Is.EqualTo(key));
        });
    }

    [Test]
    public void An_empty_value_is_written_as_empty_base64()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);

        Assert.That(Text(encoder.Entry("k", [])), Is.EqualTo("{\"key\":\"k\",\"value\":\"\",\"encoding\":\"base64\"}\n"));
    }

    [Test]
    public void The_ndjson_trailer_is_one_object_with_no_key()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);

        var line = Text(encoder.Trailer(new ExplorerDownloadTrailer(DataExportEncoder.FormatId, 2, true, ExplorerDownloadRefusal.None, null)));

        Assert.That(line, Is.EqualTo("{\"trailer\":{\"format\":\"lattice-export/1\",\"items\":2,\"complete\":true,\"refused\":\"none\",\"reason\":null}}\n"));
        Assert.That(encoder.Header().IsEmpty, Is.True, "NDJSON has no header");
    }

    [Test]
    public void Csv_writes_a_header_quoted_keys_json_values_and_base64_values()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Csv);

        var header = Text(encoder.Header());
        var json = Text(encoder.Entry("line \"7\"", Encoding.UTF8.GetBytes("{\"name\":\"Line \\\"7\\\"\"}")));
        var binary = Text(encoder.Entry("raw,key\r\n", [0xFF]));

        Assert.Multiple(() =>
        {
            Assert.That(header, Is.EqualTo("key,value,encoding\r\n"));
            Assert.That(json, Is.EqualTo("\"line \"\"7\"\"\",\"{\"\"name\"\":\"\"Line \\\"\"7\\\"\"\"\"}\",json\r\n"));
            Assert.That(binary, Is.EqualTo("\"raw,key\r\n\",/w==,base64\r\n"));
        });
    }

    [Test]
    public void The_csv_trailer_row_has_an_empty_key_the_trailer_object_and_the_trailer_marker()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Csv);

        var row = Text(encoder.Trailer(new ExplorerDownloadTrailer(DataExportEncoder.FormatId, 0, false, ExplorerDownloadRefusal.All, "denied")));

        Assert.That(row, Is.EqualTo(",\"{\"\"format\"\":\"\"lattice-export/1\"\",\"\"items\"\":0,\"\"complete\"\":false,\"\"refused\"\":\"\"all\"\",\"\"reason\"\":\"\"denied\"\"}\",trailer\r\n"));
    }

    [TestCase("\ud800")]
    [TestCase("a\udc00b")]
    [TestCase("tail\ud83d")]
    public void A_key_with_an_unpaired_surrogate_is_refused_in_either_format(string key)
    {
        using var ndjson = new DataExportEncoder(DataExportFormat.Ndjson);
        using var csv = new DataExportEncoder(DataExportFormat.Csv);

        Assert.Multiple(() =>
        {
            Assert.That(() => ndjson.Entry(key, [1]), Throws.InstanceOf<DataExportKeyException>());
            Assert.That(() => csv.Entry(key, [1]), Throws.InstanceOf<DataExportKeyException>());
        });
    }

    [Test]
    public void A_key_with_a_surrogate_pair_is_written()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Csv);

        Assert.That(Text(encoder.Entry("smile\ud83d\ude00", Encoding.UTF8.GetBytes("1"))), Is.EqualTo("\"smile\ud83d\ude00\",\"1\",json\r\n"));
    }

    [Test]
    public void The_buffer_is_reused_rather_than_growing_with_the_number_of_records()
    {
        using var encoder = new DataExportEncoder(DataExportFormat.Ndjson);
        var first = encoder.Entry("a", new byte[1000]);
        var length = first.Length;
        for (var i = 0; i < 1000; i++)
        {
            encoder.Entry("a", new byte[1000]);
        }

        Assert.That(encoder.Entry("a", new byte[1000]).Length, Is.EqualTo(length));
    }

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);
}
