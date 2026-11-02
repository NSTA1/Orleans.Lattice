using System.Text;
using Grpc.Core;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The data export (issue #5366): it pages the scope through the Data API one page at a
/// time, resumes an expired scan after the last key it wrote without a duplicate or a gap,
/// asks the facade's gate coverage once and reports it in the trailer - never a silent
/// partial - and holds one page whatever the size of the tree.
/// </summary>
[TestFixture]
public sealed class DataExportDownloadTests
{
    [Test]
    public async Task A_whole_tree_export_writes_every_entry_then_a_complete_trailer()
    {
        var source = Seed(600);
        var (text, ticket) = await RunAsync(source, Request());

        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Multiple(() =>
        {
            Assert.That(lines, Has.Length.EqualTo(601));
            Assert.That(lines[0], Is.EqualTo("{\"key\":\"key-0000\",\"value\":{\"n\":0}}"));
            Assert.That(lines[^1], Is.EqualTo("{\"trailer\":{\"format\":\"lattice-export/1\",\"items\":600,\"complete\":true,\"refused\":\"none\",\"reason\":null}}"));
            Assert.That(ticket.Items, Is.EqualTo(600));
            Assert.That(ticket.Bytes, Is.EqualTo(Encoding.UTF8.GetByteCount(text) - Encoding.UTF8.GetByteCount(lines[^1] + "\n")));
            Assert.That(source.Requests, Has.Count.EqualTo(3), "600 entries in pages of 256");
            Assert.That(source.Requests.Count(request => request.IncludeGateCoverage), Is.EqualTo(1), "the gate is asked once, on the opening read");
            Assert.That(source.Requests, Has.All.Matches<DataRangeRequest>(request => request.PageSize == DataExportDownload.PageSize));
            Assert.That(source.Disposed, Is.True);
        });
    }

    [Test]
    public async Task A_prefix_export_reads_only_the_prefix_range()
    {
        var source = Seed(10).With("other/1", "1"u8.ToArray()).With("kez", "2"u8.ToArray());

        var (text, _) = await RunAsync(source, Request() with { Scope = DataExportScopeKind.Prefix, Prefix = "key-000" });

        Assert.Multiple(() =>
        {
            Assert.That(source.Requests[0].StartInclusive, Is.EqualTo("key-000"));
            Assert.That(source.Requests[0].EndExclusive, Is.EqualTo("key-001"));
            Assert.That(text.Split('\n', StringSplitOptions.RemoveEmptyEntries), Has.Length.EqualTo(11));
            Assert.That(text, Does.Not.Contain("other/1").And.Not.Contain("kez"));
        });
    }

    [Test]
    public async Task A_range_export_reads_from_the_first_key_up_to_the_stop_key()
    {
        var source = Seed(10);

        var (text, _) = await RunAsync(source, Request() with { Scope = DataExportScopeKind.Range, From = "key-0003", To = "key-0006" });

        Assert.That(text, Does.Contain("key-0003").And.Contain("key-0005").And.Not.Contain("key-0006").And.Not.Contain("key-0002"));
    }

    [Test]
    public async Task An_expired_scan_resumes_after_the_last_key_written_with_no_duplicate_or_gap()
    {
        var source = Seed(700);
        source.ExpireNextContinuations = 1;

        var (text, _) = await RunAsync(source, Request());

        var keys = text.Split('\n', StringSplitOptions.RemoveEmptyEntries)[..^1]
            .Select(line => line.Split('"')[3])
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(Enumerable.Range(0, 700).Select(i => $"key-{i:D4}")));
            Assert.That(source.Requests[2].ContinuationToken, Is.Null, "the scan is reopened");
            Assert.That(source.Requests[2].StartInclusive, Is.EqualTo("key-0255\0"));
            Assert.That(source.Requests[2].IncludeGateCoverage, Is.False);
            Assert.That(text, Does.EndWith("\"complete\":true,\"refused\":\"none\",\"reason\":null}}\n"));
        });
    }

    [Test]
    public async Task A_scan_that_expires_on_every_page_still_completes_by_reopening()
    {
        var source = Seed(700);
        source.ExpireNextContinuations = int.MaxValue;

        var (text, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Complete, Is.True);
            Assert.That(ticket.Trailer.Items, Is.EqualTo(700));
            Assert.That(text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Distinct().Count(), Is.EqualTo(701));
        });
    }

    [Test]
    public async Task A_scan_that_keeps_expiring_without_progress_stops_and_says_why()
    {
        var store = new ExplorerDownloadTicketStore();
        var ticket = store.Mint(new DataExportDownload(Request(), () => new StallingSource()), "b");
        store.Redeem(ticket.Token, "b", out _);
        using var body = new MemoryStream();

        await ticket.Download.RunAsync(body, ticket, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Complete, Is.False);
            Assert.That(ticket.Trailer.Reason, Is.EqualTo("The scan kept expiring before it could make progress, so the export stopped."));
            Assert.That(Encoding.UTF8.GetString(body.ToArray()), Does.EndWith("\"refused\":\"unknown\",\"reason\":\"The scan kept expiring before it could make progress, so the export stopped.\"}}\n"));
        });
    }

    [Test]
    public async Task A_narrowed_grant_exports_what_it_admits_and_the_trailer_reports_some_refused()
    {
        var source = Seed(3);
        source.Coverage = LatticeRangeReadGateCoverage.Filtered;

        var (text, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Items, Is.EqualTo(3));
            Assert.That(ticket.Trailer.Complete, Is.False);
            Assert.That(ticket.Trailer.Refused, Is.EqualTo(ExplorerDownloadRefusal.Some));
            Assert.That(text, Does.EndWith("\"complete\":false,\"refused\":\"some\",\"reason\":\"Your access covers only part of this scope, so keys you may not read were left out.\"}}\n"));
        });
    }

    [Test]
    public async Task A_denied_scope_exports_nothing_and_the_trailer_reports_all_refused()
    {
        var source = Seed(3);
        source.Coverage = LatticeRangeReadGateCoverage.Denied;

        var (text, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(text.Split('\n', StringSplitOptions.RemoveEmptyEntries), Has.Length.EqualTo(1));
            Assert.That(ticket.Trailer!.Refused, Is.EqualTo(ExplorerDownloadRefusal.All));
            Assert.That(ticket.Trailer.Items, Is.Zero);
        });
    }

    [Test]
    public async Task A_tree_the_caller_cannot_observe_reports_an_unknown_refusal()
    {
        var source = Seed(3);
        source.Coverage = null;

        var (_, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Refused, Is.EqualTo(ExplorerDownloadRefusal.Unknown));
            Assert.That(ticket.Trailer.Reason, Is.EqualTo("The tree was not found, or you may not read it."));
        });
    }

    [TestCase(StatusCode.PermissionDenied, "Your access was refused part-way through, so the export stopped.")]
    [TestCase(StatusCode.Unavailable, "The cluster stopped answering part-way through, so the export stopped.")]
    [TestCase(StatusCode.Unimplemented, "This cluster does not serve the Data API, so nothing could be exported.")]
    [TestCase(StatusCode.Internal, "The download stopped because the cluster could not be read.")]
    public async Task A_transport_fault_ends_the_file_with_a_plain_reason(StatusCode status, string reason)
    {
        var source = Seed(3);
        source.Fault = new RpcException(new Status(status, "server detail"));

        var (text, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Reason, Is.EqualTo(reason));
            Assert.That(text, Does.Not.Contain("server detail"));
        });
    }

    [Test]
    public async Task An_unwritable_key_stops_the_export_after_the_keys_before_it()
    {
        var source = Seed(2).With("key-0001z\ud800", "1"u8.ToArray());

        var (text, ticket) = await RunAsync(source, Request());

        Assert.Multiple(() =>
        {
            Assert.That(ticket.Trailer!.Items, Is.EqualTo(2));
            Assert.That(ticket.Trailer.Reason, Is.EqualTo("A key is not valid Unicode text, so the export stopped before it."));
            Assert.That(text.Split('\n', StringSplitOptions.RemoveEmptyEntries), Has.Length.EqualTo(3));
        });
    }

    [Test]
    public async Task A_csv_export_starts_with_its_header_and_ends_with_its_trailer_row()
    {
        var source = Seed(2);

        var (text, _) = await RunAsync(source, Request() with { Format = DataExportFormat.Csv });

        Assert.That(text, Is.EqualTo(
            "key,value,encoding\r\n"
            + "\"key-0000\",\"{\"\"n\"\":0}\",json\r\n"
            + "\"key-0001\",\"{\"\"n\"\":1}\",json\r\n"
            + ",\"{\"\"format\"\":\"\"lattice-export/1\"\",\"\"items\"\":2,\"\"complete\"\":true,\"\"refused\"\":\"\"none\"\",\"\"reason\"\":null}\",trailer\r\n"));
    }

    [Test]
    public void The_resume_key_is_the_smallest_key_after_the_last_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataExportDownload.After("a"), Is.EqualTo("a\0"));
            Assert.That(string.CompareOrdinal(DataExportDownload.After("a"), "a"), Is.Positive);
            Assert.That(string.CompareOrdinal(DataExportDownload.After("a"), "a\u0001"), Is.Negative);
        });
    }

    [Test]
    public void The_download_takes_its_name_and_type_from_the_request()
    {
        var download = new DataExportDownload(Request() with { Format = DataExportFormat.Csv }, () => new FakeDataExportSource());

        Assert.Multiple(() =>
        {
            Assert.That(download.FileName, Is.EqualTo("factory-orders.csv"));
            Assert.That(download.ContentType, Is.EqualTo("text/csv; charset=utf-8"));
            Assert.That(download.Request.TreeId, Is.EqualTo("factory-orders"));
            Assert.That(() => new DataExportDownload(null!, () => new FakeDataExportSource()), Throws.ArgumentNullException);
            Assert.That(() => new DataExportDownload(Request(), null!), Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// The bounded-memory proof: 200 MB of entries stream through the export while the
    /// managed heap stays within the same 64 MB ceiling G1's import is held to. An export
    /// that buffered the file would hold all 200 MB at its end.
    /// </summary>
    [Test]
    public async Task A_200_megabyte_export_streams_within_a_64_megabyte_heap_ceiling()
    {
        const long target = 200L * 1024 * 1024;
        const long ceiling = 64L * 1024 * 1024;
        using var source = new GeneratedSource(entrySize: 64 * 1024, entries: (int)(target / (64 * 1024)));
        using var sink = new MeasuringStream();
        var baseline = GC.GetTotalMemory(forceFullCollection: true);
        source.OnPage = () => sink.Sample();

        var store = new ExplorerDownloadTicketStore();
        var ticket = store.Mint(new DataExportDownload(Request(), () => source), "b");
        store.Redeem(ticket.Token, "b", out _);
        await ticket.Download.RunAsync(sink, ticket, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sink.Written, Is.GreaterThan(target), "the export wrote the whole 200 MB");
            Assert.That(ticket.Trailer!.Complete, Is.True);
            Assert.That(sink.PeakHeap - baseline, Is.LessThan(ceiling), $"peak heap grew by {(sink.PeakHeap - baseline) / (1024 * 1024)} MB");
        });
    }

    private static DataExportRequest Request() => new() { TreeId = "factory-orders", TreeName = "factory-orders" };

    private static FakeDataExportSource Seed(int count)
    {
        var source = new FakeDataExportSource();
        for (var i = 0; i < count; i++)
        {
            source.With($"key-{i:D4}", Encoding.UTF8.GetBytes($"{{\"n\":{i}}}"));
        }

        return source;
    }

    private static async Task<(string Text, ExplorerDownloadTicket Ticket)> RunAsync(FakeDataExportSource source, DataExportRequest request)
    {
        var store = new ExplorerDownloadTicketStore();
        var ticket = store.Mint(new DataExportDownload(request, () => source), "b");
        store.Redeem(ticket.Token, "b", out _);
        using var body = new MemoryStream();
        await ticket.Download.RunAsync(body, ticket, CancellationToken.None);
        return (Encoding.UTF8.GetString(body.ToArray()), ticket);
    }

    /// <summary>A scan whose every page is empty and whose every continuation has expired.</summary>
    private sealed class StallingSource : IDataExportSource
    {
        public Task<DataRangePage> ReadAsync(DataRangeRequest request, CancellationToken cancellationToken) =>
            string.IsNullOrEmpty(request.ContinuationToken)
                ? Task.FromResult(new DataRangePage { TreeId = request.TreeId, ContinuationToken = "stalled" })
                : throw new DataExportCursorExpiredException();

        public void Dispose()
        {
        }
    }

    /// <summary>Generates entries on demand and retains none of them.</summary>
    private sealed class GeneratedSource(int entrySize, int entries) : IDataExportSource
    {
        public Action? OnPage { get; set; }

        public Task<DataRangePage> ReadAsync(DataRangeRequest request, CancellationToken cancellationToken)
        {
            OnPage?.Invoke();
            var next = request.ContinuationToken is { } token ? int.Parse(token, System.Globalization.CultureInfo.InvariantCulture) : 0;
            var page = new List<DataEntry>(request.PageSize);
            for (var i = next; i < entries && page.Count < request.PageSize; i++)
            {
                var value = new byte[entrySize];
                value.AsSpan().Fill((byte)'x');
                value[0] = (byte)'"';
                value[^1] = (byte)'"';
                page.Add(new DataEntry { Key = $"key-{i:D8}", Value = value });
            }

            var end = next + page.Count;
            return Task.FromResult(new DataRangePage
            {
                TreeId = request.TreeId,
                Entries = page,
                ContinuationToken = end < entries ? end.ToString(System.Globalization.CultureInfo.InvariantCulture) : null,
                GateCoverage = request.IncludeGateCoverage ? LatticeRangeReadGateCoverage.Unrestricted : null,
            });
        }

        public void Dispose()
        {
        }
    }

    /// <summary>A response body that keeps nothing: it counts bytes and samples the heap.</summary>
    private sealed class MeasuringStream : Stream
    {
        public long Written { get; private set; }

        public long PeakHeap { get; private set; }

        public void Sample() => PeakHeap = Math.Max(PeakHeap, GC.GetTotalMemory(forceFullCollection: true));

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => Written;

        public override long Position { get => Written; set => throw new NotSupportedException(); }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => Written += count;

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Written += buffer.Length;
            return ValueTask.CompletedTask;
        }
    }
}
