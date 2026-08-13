using System.Runtime.CompilerServices;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Reads a repository-context snapshot stream produced by
/// <see cref="RepoContextSnapshotWriter"/>: it validates the versioned header,
/// then yields each <see cref="RepoContextSnapshotRecord"/> in stream order. The
/// reader pulls one frame at a time, so a large snapshot is consumed without
/// materializing every record in memory.
/// </summary>
internal sealed class RepoContextSnapshotReader
{
    private readonly Stream _source;
    private readonly Serializer _serializer;

    /// <summary>Creates a reader over <paramref name="source"/>.</summary>
    /// <param name="source">The stream to read the snapshot from. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer. Must not be <see langword="null"/>.</param>
    public RepoContextSnapshotReader(Stream source, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(serializer);
        _source = source;
        _serializer = serializer;
    }

    /// <summary>The format version read from the stream header, once <see cref="ReadAsync"/> has started.</summary>
    public int FormatVersion { get; private set; }

    /// <summary>
    /// Validates the header, then streams every record in the snapshot. Throws
    /// <see cref="InvalidDataException"/> when the header marker is missing, the
    /// format version is unsupported, or a frame is truncated.
    /// </summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The records, in stream order.</returns>
    public async IAsyncEnumerable<RepoContextSnapshotRecord> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        FormatVersion = await RepoContextSnapshotFormat
            .ReadHeaderAsync(_source, cancellationToken)
            .ConfigureAwait(false);

        while (true)
        {
            var payload = await RepoContextSnapshotFormat
                .ReadFrameAsync(_source, cancellationToken)
                .ConfigureAwait(false);
            if (payload is null)
            {
                yield break;
            }

            yield return _serializer.Deserialize<RepoContextSnapshotRecord>(payload);
        }
    }
}
