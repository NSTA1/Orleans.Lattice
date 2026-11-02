using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The full list of lines an import refused (issue #5365), streamed through the download
/// seam as NDJSON: one <c>{"line","key","code","reason"}</c> object per refused line, then
/// the trailer. It copies the import's rejection file a buffer at a time, so it holds no
/// more of the list than one read.
/// </summary>
/// <param name="log">The import's rejections.</param>
/// <param name="treeName">The tree the import wrote to, for the file name.</param>
internal sealed class DataImportRejectionsDownload(DataImportRejectionLog log, string treeName) : ExplorerDownload
{
    /// <summary>The format id the file's trailer carries.</summary>
    public const string FormatId = "lattice-import-rejections/1";

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <inheritdoc />
    public override string FileName => DataExportRequest.SafeFileStem(treeName) + "-import-rejections.ndjson";

    /// <inheritdoc />
    public override string ContentType => "application/x-ndjson";

    /// <inheritdoc />
    protected override string Format => FormatId;

    /// <inheritdoc />
    protected override async Task<ExplorerDownloadTrailer> WriteBodyAsync(Stream body, ExplorerDownloadTicket ticket, CancellationToken cancellationToken)
    {
        await using var source = log.OpenRead();
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        long items = 0;
        long bytes = 0;
        try
        {
            int read;
            while ((read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
            {
                await body.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                items += buffer.AsSpan(0, read).Count((byte)'\n');
                bytes += read;
                ticket.Report(items, bytes);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return new ExplorerDownloadTrailer(FormatId, items, Complete: true, ExplorerDownloadRefusal.None, Reason: null);
    }

    /// <inheritdoc />
    protected override async ValueTask WriteTrailerAsync(Stream body, ExplorerDownloadTrailer trailer, CancellationToken cancellationToken)
    {
        var line = new ArrayBufferWriter<byte>(256);
        await using (var writer = new Utf8JsonWriter(line, WriterOptions))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(DataExportEncoder.TrailerProperty);
            trailer.WriteJson(writer);
            writer.WriteEndObject();
        }

        line.Write("\n"u8);
        await body.WriteAsync(line.WrittenMemory, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override string DescribeFailure(Exception exception) =>
        exception is ObjectDisposedException
            ? "The import's dialog was closed, so its list of refused lines is gone."
            : "The list of refused lines could not be read.";
}
