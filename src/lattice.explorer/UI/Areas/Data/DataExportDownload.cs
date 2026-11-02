using Grpc.Core;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// A data export (issue #5366): the keys of one tree in a scope, streamed in pages
/// through the Data API as the user who prepared it, encoded in
/// <c>lattice-export/1</c> NDJSON or CSV, and ended with a trailer that says whether
/// the file is complete and whether the user's access withheld anything.
/// </summary>
/// <remarks>
/// <para>
/// <b>One page at a time.</b> A page is read, written to the response record by record,
/// and dropped before the next is read, so the export holds one page and one encoded
/// record whatever the size of the tree.
/// </para>
/// <para>
/// <b>Resumes an expired scan.</b> When the scan's cursor expires mid-export, the scan is
/// reopened just after the last key written - the same recovery the keys panel makes - so
/// no key is written twice and none is skipped. A scan that keeps expiring without
/// progress stops, and the trailer says why.
/// </para>
/// <para>
/// <b>Never a silent partial.</b> The first read asks the facade how much of the scope the
/// user's access admits. A scope the access narrows ends with <c>refused: some</c> and
/// <c>complete: false</c>; one it denies, with <c>refused: all</c>. That is a
/// classification, never a count of withheld keys, because a count would disclose how
/// much data the user may not read.
/// </para>
/// </remarks>
internal sealed class DataExportDownload : ExplorerDownload
{
    /// <summary>How many entries each page reads.</summary>
    internal const int PageSize = 256;

    /// <summary>How many times in a row an expired scan is reopened without progress before the export stops.</summary>
    internal const int MaxResumesWithoutProgress = 3;

    private readonly DataExportRequest _request;
    private readonly Func<IDataExportSource> _openSource;

    /// <summary>Creates the export.</summary>
    /// <param name="request">What to export.</param>
    /// <param name="openSource">Opens the source the export reads, once, when the download starts.</param>
    public DataExportDownload(DataExportRequest request, Func<IDataExportSource> openSource)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(openSource);
        _request = request;
        _openSource = openSource;
    }

    /// <summary>What the export covers.</summary>
    public DataExportRequest Request => _request;

    /// <inheritdoc />
    public override string FileName => _request.FileName;

    /// <inheritdoc />
    public override string ContentType => _request.ContentType;

    /// <inheritdoc />
    protected override string Format => DataExportEncoder.FormatId;

    /// <inheritdoc />
    protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
    {
        using var source = _openSource();
        using var encoder = new DataExportEncoder(_request.Format);

        long items = 0;
        long bytes = 0;
        var header = encoder.Header();
        if (!header.IsEmpty)
        {
            await body.WriteAsync(header, cancellationToken).ConfigureAwait(false);
            bytes += header.Length;
        }

        var (start, end) = _request.Bounds;
        LatticeRangeReadGateCoverage? coverage = null;
        var opened = false;
        string? continuation = null;
        string? lastKey = null;
        var resumes = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var request = continuation is not null
                ? new DataRangeRequest { TreeId = _request.TreeId, ContinuationToken = continuation, PageSize = PageSize }
                : new DataRangeRequest
                {
                    TreeId = _request.TreeId,
                    StartInclusive = lastKey is null ? start : After(lastKey),
                    EndExclusive = end,
                    PageSize = PageSize,
                    IncludeGateCoverage = !opened,
                };

            DataRangePage page;
            try
            {
                page = await source.ReadAsync(request, cancellationToken).ConfigureAwait(false);
            }
            catch (DataExportCursorExpiredException) when (resumes < MaxResumesWithoutProgress)
            {
                resumes++;
                continuation = null;
                continue;
            }

            if (!opened)
            {
                opened = true;
                coverage = page.GateCoverage;
            }

            foreach (var entry in page.Entries)
            {
                ReadOnlyMemory<byte> record;
                try
                {
                    record = encoder.Entry(entry.Key, entry.Value);
                }
                catch (DataExportKeyException)
                {
                    // The trailer counts the entries written before the one that stopped it.
                    ticket.Report(items, bytes);
                    throw;
                }

                await body.WriteAsync(record, cancellationToken).ConfigureAwait(false);
                bytes += record.Length;
                items++;
                lastKey = entry.Key;
            }

            if (page.Entries.Count > 0)
            {
                resumes = 0;
                await body.FlushAsync(cancellationToken).ConfigureAwait(false);
                ticket.Report(items, bytes);
            }

            continuation = page.ContinuationToken;
            if (string.IsNullOrEmpty(continuation))
            {
                break;
            }
        }

        ticket.Report(items, bytes);
        return TrailerFor(coverage, items);
    }

    /// <inheritdoc />
    protected override async ValueTask WriteTrailerAsync(Stream body, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken)
    {
        using var encoder = new DataExportEncoder(_request.Format);
        await body.WriteAsync(encoder.Trailer(trailer), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override string DescribeFailure(Exception exception) => exception switch
    {
        DataExportKeyException => "A key is not valid Unicode text, so the export stopped before it.",
        DataExportCursorExpiredException => "The scan kept expiring before it could make progress, so the export stopped.",
        RpcException { StatusCode: StatusCode.PermissionDenied or StatusCode.Unauthenticated }
            => "Your access was refused part-way through, so the export stopped.",
        RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded }
            => "The cluster stopped answering part-way through, so the export stopped.",
        RpcException { StatusCode: StatusCode.Unimplemented }
            => "This cluster does not serve the Data API, so nothing could be exported.",
        _ => base.DescribeFailure(exception),
    };

    /// <summary>The trailer for a scan that ran to its end under <paramref name="coverage"/>.</summary>
    /// <param name="coverage">What the facade reported the user's access admits, or <see langword="null"/> when it reported nothing.</param>
    /// <param name="items">How many entries were written.</param>
    internal static ExplorerDownloadTrailer TrailerFor(LatticeRangeReadGateCoverage? coverage, long items) => coverage switch
    {
        LatticeRangeReadGateCoverage.Unrestricted =>
            new(DataExportEncoder.FormatId, items, Complete: true, ExplorerDownloadRefusal.None, Reason: null),
        LatticeRangeReadGateCoverage.Filtered =>
            new(DataExportEncoder.FormatId, items, Complete: false, ExplorerDownloadRefusal.Some,
                "Your access covers only part of this scope, so keys you may not read were left out."),
        LatticeRangeReadGateCoverage.Denied =>
            new(DataExportEncoder.FormatId, items, Complete: false, ExplorerDownloadRefusal.All,
                "Your access does not allow reading this scope, so nothing was exported."),
        _ => new(DataExportEncoder.FormatId, items, Complete: false, ExplorerDownloadRefusal.Unknown,
            "The tree was not found, or you may not read it."),
    };

    /// <summary>The smallest key that sorts after <paramref name="key"/>, where a reopened scan resumes.</summary>
    /// <param name="key">The last key written.</param>
    internal static string After(string key) => key + '\0';
}
