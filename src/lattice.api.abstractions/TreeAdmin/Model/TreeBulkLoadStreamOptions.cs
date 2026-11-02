namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Shapes a streamed bulk load through
/// <see cref="ILatticeTreeAdmin.BulkLoadStreamAsync"/>: how many entries each chunk
/// carries, the chunk index the stream starts at, and - for a transport binding that
/// streams the chunks over the wire - how many chunks may be in flight unacknowledged
/// and how many times a broken stream is resumed.
/// </summary>
/// <remarks>
/// Every bound is a memory bound as much as a throughput one: a streamed load holds at
/// most one chunk being filled plus <see cref="MaxUnacknowledgedChunks"/> chunks awaiting
/// their acknowledgement, whatever the size of the input.
/// </remarks>
public sealed record TreeBulkLoadStreamOptions
{
    /// <summary>The default number of entries per chunk.</summary>
    public const int DefaultChunkSize = 1_000;

    /// <summary>The largest number of entries one chunk may carry.</summary>
    public const int MaxChunkSize = 10_000;

    /// <summary>The default number of chunks a transport keeps in flight unacknowledged.</summary>
    public const int DefaultMaxUnacknowledgedChunks = 4;

    /// <summary>The default number of times a transport resumes a broken stream.</summary>
    public const int DefaultMaxResumeAttempts = 3;

    /// <summary>The options a caller gets when it passes none.</summary>
    public static TreeBulkLoadStreamOptions Default { get; } = new();

    /// <summary>
    /// How many entries each chunk carries, from 1 to <see cref="MaxChunkSize"/>.
    /// Defaults to <see cref="DefaultChunkSize"/>.
    /// </summary>
    public int ChunkSize { get; init; } = DefaultChunkSize;

    /// <summary>
    /// The chunk index the first chunk of this stream is appended at. Zero (the default)
    /// opens the session first, which requires the tree to be empty; a positive index
    /// resumes an existing session under the same operation id, skipping the open, so a
    /// caller that kept its own record of the last acknowledged chunk can continue a load
    /// after a fault by re-streaming its input from that chunk on.
    /// </summary>
    public long FirstChunkIndex { get; init; }

    /// <summary>
    /// For a transport that streams chunks over the wire, how many chunks may be sent and
    /// not yet acknowledged before the sender waits, from 1 to 64. Defaults to
    /// <see cref="DefaultMaxUnacknowledgedChunks"/>. An in-process implementation ignores it.
    /// </summary>
    public int MaxUnacknowledgedChunks { get; init; } = DefaultMaxUnacknowledgedChunks;

    /// <summary>
    /// For a transport that streams chunks over the wire, how many times a stream broken
    /// by a transient transport fault is reopened and continued from the first
    /// unacknowledged chunk, from 0 to 16. Defaults to <see cref="DefaultMaxResumeAttempts"/>.
    /// An in-process implementation ignores it.
    /// </summary>
    public int MaxResumeAttempts { get; init; } = DefaultMaxResumeAttempts;

    /// <summary>Throws when any bound is out of its documented range.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A bound is out of range.</exception>
    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ChunkSize, 1, nameof(ChunkSize));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ChunkSize, MaxChunkSize, nameof(ChunkSize));
        ArgumentOutOfRangeException.ThrowIfNegative(FirstChunkIndex, nameof(FirstChunkIndex));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxUnacknowledgedChunks, 1, nameof(MaxUnacknowledgedChunks));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxUnacknowledgedChunks, 64, nameof(MaxUnacknowledgedChunks));
        ArgumentOutOfRangeException.ThrowIfNegative(MaxResumeAttempts, nameof(MaxResumeAttempts));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxResumeAttempts, 16, nameof(MaxResumeAttempts));
    }
}
