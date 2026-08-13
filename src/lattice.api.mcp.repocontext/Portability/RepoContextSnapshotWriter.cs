using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Streams <see cref="RepoContextSnapshotRecord"/> instances to a destination
/// stream in the repository-context snapshot format: it writes the versioned
/// header once, then one length-prefixed, Orleans-serialized frame per record.
/// The writer holds no buffering of its own beyond the current frame, so a large
/// export flows straight through to the destination stream.
/// </summary>
internal sealed class RepoContextSnapshotWriter
{
    private readonly Stream _destination;
    private readonly Serializer _serializer;
    private bool _headerWritten;

    /// <summary>Creates a writer over <paramref name="destination"/>.</summary>
    /// <param name="destination">The stream to write the snapshot to. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer. Must not be <see langword="null"/>.</param>
    public RepoContextSnapshotWriter(Stream destination, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(serializer);
        _destination = destination;
        _serializer = serializer;
    }

    /// <summary>
    /// Writes the stream header if it has not been written yet. Idempotent within
    /// a single writer: subsequent calls are no-ops. Called automatically by
    /// <see cref="WriteRecordAsync"/>, so a header is emitted even for an empty
    /// snapshot when this is called first.
    /// </summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    public async ValueTask WriteHeaderAsync(CancellationToken cancellationToken = default)
    {
        if (_headerWritten)
        {
            return;
        }

        await RepoContextSnapshotFormat
            .WriteHeaderAsync(_destination, RepoContextSnapshotFormat.CurrentVersion, cancellationToken)
            .ConfigureAwait(false);
        _headerWritten = true;
    }

    /// <summary>
    /// Writes one record frame, emitting the header first if necessary.
    /// </summary>
    /// <param name="record">The record to write. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    public async ValueTask WriteRecordAsync(
        RepoContextSnapshotRecord record,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);

        var payload = _serializer.SerializeToArray(record);        await RepoContextSnapshotFormat
            .WriteFrameAsync(_destination, payload, cancellationToken)
            .ConfigureAwait(false);
    }
}
