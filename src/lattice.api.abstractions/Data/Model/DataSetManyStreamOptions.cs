namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Shapes a streamed write through <see cref="ILatticeDataApi.SetManyStreamAsync"/>: how
/// many entries each batch carries and the ordinal the stream's first entry is numbered
/// with in the rejections it reports.
/// </summary>
public sealed record DataSetManyStreamOptions
{
    /// <summary>The default number of entries per batch.</summary>
    public const int DefaultBatchSize = 256;

    /// <summary>The largest number of entries one batch may carry.</summary>
    public const int MaxBatchSize = 10_000;

    /// <summary>The options a caller gets when it passes none.</summary>
    public static DataSetManyStreamOptions Default { get; } = new();

    /// <summary>
    /// How many entries each batch carries, from 1 to <see cref="MaxBatchSize"/>. A batch is
    /// written with one <see cref="ILatticeDataApi.SetManyAsync"/> call, and memory is
    /// bounded by it. Defaults to <see cref="DefaultBatchSize"/>.
    /// </summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>
    /// The ordinal of the stream's first entry, which every
    /// <see cref="DataEntryRejection.Ordinal"/> counts from. Zero by default; a caller that
    /// writes one long input in several streams passes the count already written.
    /// </summary>
    public long FirstOrdinal { get; init; }

    /// <summary>Throws when any bound is out of its documented range.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A bound is out of range.</exception>
    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(BatchSize, MaxBatchSize, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfNegative(FirstOrdinal, nameof(FirstOrdinal));
    }
}
