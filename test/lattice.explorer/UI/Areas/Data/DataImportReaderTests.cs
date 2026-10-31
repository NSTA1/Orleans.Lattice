using System.Text;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The import readers (issue #5365): key templates and JSON Pointers, the bounded line
/// reader, and each format's record producer - NDJSON, CSV, <c>lattice-export/1</c> and
/// <c>key=value</c> - including every way a line is refused and every way a file is.
/// </summary>
[TestFixture]
public sealed class DataImportReaderTests
{
    [TestCase("/id", true)]
    [TestCase("order/{/id}", true)]
    [TestCase("id", false)]
    [TestCase("order/{id}-{line}", false)]
    [TestCase("{{literal}}/{id}", false)]
    public void Key_template_parses(string text, bool pointers)
    {
        Assert.That(DataImportKeyTemplate.TryParse(text, pointers, out var template, out var error), Is.True, error);
        Assert.That(template!.References, Is.Not.Empty);
    }

    [TestCase("", true, "Say where the key comes from")]
    [TestCase("id", true, "is not a JSON Pointer")]
    [TestCase("order/{id}", true, "is not a JSON Pointer")]
    [TestCase("order/{/id", true, "no closing")]
    [TestCase("order/}x", false, "no opening")]
    [TestCase("order/{}", false, "empty {}")]
    [TestCase("{{fixed}}", false, "reads no field")]
    [TestCase("/a~2b", true, "~ must be written ~0")]
    public void Key_template_refuses(string text, bool pointers, string reason)
    {
        Assert.That(DataImportKeyTemplate.TryParse(text, pointers, out _, out var error), Is.False);
        Assert.That(error, Does.Contain(reason));
    }

    [Test]
    public void Key_template_builds_literals_and_fields_in_order()
    {
        DataImportKeyTemplate.TryParse("{{o}}/{a}-{b}", false, out var template, out _);
        var fields = new[] { "1", "x" };

        Assert.That(template!.TryBuild(i => (fields[i], null), out var key, out _), Is.True);
        Assert.That(key, Is.EqualTo("{o}/1-x"));
        Assert.That(template.TryBuild(_ => (null, "missing"), out _, out var error), Is.False);
        Assert.That(error, Is.EqualTo("missing"));
    }

    [Test]
    public async Task Ndjson_reads_keys_from_pointers_and_keeps_the_line_verbatim()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.Ndjson, KeyTemplate = "order/{/id}" },
            "{\"id\":7,\"total\":1.5}\n\n  \n{\"id\":\"x\"}\r\n");

        Assert.Multiple(() =>
        {
            Assert.That(records.Select(r => (r.Line, r.Entry?.Key)), Is.EqualTo(new[] { (1L, (string?)"order/7"), (4L, "order/x") }));
            Assert.That(Encoding.UTF8.GetString(records[0].Entry!.Value), Is.EqualTo("{\"id\":7,\"total\":1.5}"));
            Assert.That(Encoding.UTF8.GetString(records[1].Entry!.Value), Is.EqualTo("{\"id\":\"x\"}"));
        });
    }

    [Test]
    public async Task Ndjson_reads_the_value_at_a_pointer()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.Ndjson, KeyTemplate = "/k/0", ValueSource = DataImportValueSource.Field, ValueField = "/v" },
            "{\"k\":[\"a\"],\"v\":{\"x\": [1, 2]}}\n");

        Assert.That(records.Single().Entry!.Key, Is.EqualTo("a"));
        Assert.That(Encoding.UTF8.GetString(records.Single().Entry!.Value), Is.EqualTo("{\"x\": [1, 2]}"));
    }

    [TestCase("not json", "not one JSON value")]
    [TestCase("{\"id\":1} {\"id\":2}", "not one JSON value")]
    [TestCase("{\"other\":1}", "no value at /id")]
    [TestCase("{\"id\":{\"a\":1}}", "is an object")]
    [TestCase("{\"id\":null}", "is null")]
    [TestCase("{\"id\":\"\"}", "The key is empty")]
    public async Task Ndjson_refuses_a_line_and_reads_on(string line, string reason)
    {
        var records = await ReadAsync(new DataImportSettings { Format = DataImportFormat.Ndjson, KeyTemplate = "/id" }, line + "\n{\"id\":2}\n");

        Assert.Multiple(() =>
        {
            Assert.That(records[0].Entry, Is.Null);
            Assert.That(records[0].Error, Does.Contain(reason));
            Assert.That(records[1].Entry!.Key, Is.EqualTo("2"));
        });
    }

    [Test]
    public async Task Csv_reads_quoted_fields_line_breaks_and_a_byte_order_mark()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "order/{id}" },
            "\uFEFFid,note,total\r\n1,\"a, \"\"b\"\"\",3\r\n2,\"two\r\nlines\",-4.5\r\n");

        Assert.Multiple(() =>
        {
            Assert.That(records.Select(r => (r.Line, r.Entry!.Key)), Is.EqualTo(new[] { (2L, "order/1"), (3L, "order/2") }));
            Assert.That(Encoding.UTF8.GetString(records[0].Entry!.Value), Is.EqualTo("{\"id\":\"1\",\"note\":\"a, \\\"b\\\"\",\"total\":\"3\"}"));
            Assert.That(Encoding.UTF8.GetString(records[1].Entry!.Value), Is.EqualTo("{\"id\":\"2\",\"note\":\"two\\r\\nlines\",\"total\":\"-4.5\"}"));
        });
    }

    [Test]
    public async Task Csv_infers_numbers_and_booleans_only_when_asked()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "id", InferTypes = true, Delimiter = ';' },
            "id;n;b;s\nk;-1.5e3;true;01\n");

        Assert.That(Encoding.UTF8.GetString(records.Single().Entry!.Value), Is.EqualTo("{\"id\":\"k\",\"n\":-1.5e3,\"b\":true,\"s\":\"01\"}"));
    }

    [Test]
    public async Task Csv_stores_one_column_as_the_value()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "id", ValueSource = DataImportValueSource.Field, ValueField = "text" },
            "id,text\nk,caf\u00e9\n");

        Assert.That(records.Single().Entry!.Value, Is.EqualTo(Encoding.UTF8.GetBytes("caf\u00e9")));
    }

    [Test]
    public async Task Csv_refuses_a_row_with_the_wrong_field_count_and_reads_on()
    {
        var records = await ReadAsync(new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "id" }, "id,v\n1\n2,b\n3,\"x\"y\n");

        Assert.Multiple(() =>
        {
            Assert.That(records[0].Error, Does.Contain("1 field; the header has 2"));
            Assert.That(records[1].Entry!.Key, Is.EqualTo("2"));
            Assert.That(records[2].Error, Does.Contain("after the closing quote"));
        });
    }

    [TestCase("", "file is empty")]
    [TestCase("id,id\n1,2\n", "twice")]
    [TestCase("id,\n1,2\n", "empty column name")]
    [TestCase("x,y\n1,2\n", "does not have")]
    public void Csv_refuses_a_file_without_a_usable_header(string text, string reason)
    {
        var error = Assert.ThrowsAsync<DataImportFileException>(() => ReadAsync(new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "id" }, text));

        Assert.That(error!.Message, Does.Contain(reason));
    }

    [Test]
    public async Task Key_value_lines_split_at_the_first_equals_sign()
    {
        var records = await ReadAsync(new DataImportSettings { Format = DataImportFormat.KeyValue }, "a=1=2\nnope\n=x\n");

        Assert.Multiple(() =>
        {
            Assert.That(records[0].Entry!.Key, Is.EqualTo("a"));
            Assert.That(Encoding.UTF8.GetString(records[0].Entry!.Value), Is.EqualTo("1=2"));
            Assert.That(records[1].Error, Does.Contain("not key=value"));
            Assert.That(records[2].Error, Does.Contain("not key=value"));
        });
    }

    [Test]
    public async Task A_lattice_export_round_trips_every_value_exactly()
    {
        byte[][] values =
        [
            Encoding.UTF8.GetBytes("{\"a\":1}"),
            [0, 255, 10, 13],
            Encoding.UTF8.GetBytes(" spaced "),
            Encoding.UTF8.GetBytes("\"text\""),
            [],
        ];
        var file = new MemoryStream();
        using (var encoder = new DataExportEncoder(DataExportFormat.Ndjson))
        {
            for (var i = 0; i < values.Length; i++)
            {
                file.Write(encoder.Entry("k" + i + "\"/\u00e9", values[i]).Span);
            }

            file.Write(encoder.Trailer(new(DataExportEncoder.FormatId, values.Length, true, Orleans.Lattice.Explorer.UI.Downloads.ExplorerDownloadRefusal.None, null)).Span);
        }

        var reader = new DataImportReader(new DataImportSettings { Format = DataImportFormat.LatticeExport });
        var records = new List<DataImportRecord>();
        file.Position = 0;
        await foreach (var record in reader.ReadAsync(file))
        {
            records.Add(record);
        }

        Assert.Multiple(() =>
        {
            Assert.That(records.Select(r => r.Entry!.Key), Is.EqualTo(Enumerable.Range(0, values.Length).Select(i => "k" + i + "\"/\u00e9")));
            Assert.That(records.Select(r => r.Entry!.Value), Is.EqualTo(values));
            Assert.That(reader.TrailerSeen, Is.True);
            Assert.That(reader.TrailerComplete, Is.True);
        });
    }

    [TestCase("[1]", "not a JSON object")]
    [TestCase("{\"value\":1}", "no \"key\"")]
    [TestCase("{\"key\":\"k\"}", "no \"value\"")]
    [TestCase("{\"key\":\"k\",\"value\":\"@@\",\"encoding\":\"base64\"}", "not valid base64")]
    [TestCase("{\"key\":\"k\",\"value\":1,\"encoding\":\"hex\"}", "only encoding")]
    public async Task A_lattice_export_line_that_is_not_an_entry_is_refused(string line, string reason)
    {
        var records = await ReadAsync(new DataImportSettings { Format = DataImportFormat.LatticeExport }, line + "\n");

        Assert.That(records.Single().Error, Does.Contain(reason));
    }

    [Test]
    public async Task A_line_after_the_export_trailer_is_refused()
    {
        var records = await ReadAsync(
            new DataImportSettings { Format = DataImportFormat.LatticeExport },
            "{\"trailer\":{\"complete\":false}}\n{\"key\":\"k\",\"value\":1}\n");

        Assert.That(records.Single().Error, Does.Contain("follows the export's trailer"));
    }

    [Test]
    public async Task The_line_reader_assembles_a_line_spanning_many_reads_and_counts_lines()
    {
        var longLine = new string('x', DataImportLineReader.ReadSize * 3 + 17);
        using var reader = new DataImportLineReader(new MemoryStream(Encoding.UTF8.GetBytes("a\r\n" + longLine + "\nlast")), DataImportReader.DefaultMaxLineBytes);
        var lines = new List<(long, int, bool)>();
        while (await reader.ReadLineAsync(CancellationToken.None))
        {
            lines.Add((reader.LineNumber, reader.Current.Length, reader.EndedWithLineFeed));
        }

        Assert.That(lines, Is.EqualTo(new[] { (1L, 1, true), (2L, longLine.Length, true), (3L, 4, false) }));
    }

    [Test]
    public void A_line_longer_than_the_bound_stops_the_file_with_its_line_number()
    {
        var text = "ok\n" + new string('x', 5000) + "\n";

        var error = Assert.ThrowsAsync<DataImportFileException>(() =>
            ReadAsync(new DataImportSettings { Format = DataImportFormat.KeyValue }, text, maxLineBytes: 1024));

        Assert.Multiple(() =>
        {
            Assert.That(error!.Line, Is.EqualTo(2));
            Assert.That(error.Message, Does.Contain("Line 2 is longer than"));
        });
    }

    [Test]
    public void Settings_validate_the_key_and_value_sources()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new DataImportSettings { KeyTemplate = "/id" }.Validate(), Is.Null);
            Assert.That(new DataImportSettings { KeyTemplate = "id" }.Validate(), Does.Contain("JSON Pointer"));
            Assert.That(new DataImportSettings { KeyTemplate = "/id", ValueSource = DataImportValueSource.Field }.Validate(), Does.Contain("Say which value"));
            Assert.That(new DataImportSettings { Format = DataImportFormat.Csv, KeyTemplate = "id", Delimiter = 'x' }.Validate(), Does.Contain("delimiter"));
            Assert.That(new DataImportSettings { Format = DataImportFormat.KeyValue }.Validate(), Is.Null);
            Assert.That(new DataImportSettings { Format = DataImportFormat.LatticeExport, KeyTemplate = "ignored" }.Validate(), Is.Null);
        });
    }

    [TestCase("0", true)]
    [TestCase("-12.5", true)]
    [TestCase("1e-9", true)]
    [TestCase("01", false)]
    [TestCase("1.", false)]
    [TestCase("+1", false)]
    [TestCase("", false)]
    [TestCase("NaN", false)]
    public void Json_number_inference_follows_RFC_8259(string text, bool number) =>
        Assert.That(DataImportReader.IsJsonNumber(text), Is.EqualTo(number));

    private static async Task<List<DataImportRecord>> ReadAsync(DataImportSettings settings, string text, int maxLineBytes = DataImportReader.DefaultMaxLineBytes)
    {
        var reader = new DataImportReader(settings, maxLineBytes);
        var records = new List<DataImportRecord>();
        await foreach (var record in reader.ReadAsync(new MemoryStream(Encoding.UTF8.GetBytes(text))))
        {
            records.Add(record);
        }

        return records;
    }
}
