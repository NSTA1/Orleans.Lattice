namespace Orleans.Lattice.Api.State;

/// <summary>
/// The retention shape that was actually applied to a single history revision
/// when it was written, carried per row so a consumer can detect a
/// retention-configuration transition by diffing adjacent revisions of the same
/// key (the "retention changed here" boundary). Retention is forward-only and
/// stamped per revision: already-written rows keep the shape they were written
/// with, and a configuration change is absorbed forward.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.RevisionRetention)]
[Immutable]
public readonly record struct RevisionRetention
{
    /// <summary>
    /// The retention mode the maintainer applied when it wrote this revision.
    /// </summary>
    [Id(0)] public HistoryRetentionMode Mode { get; init; }

    /// <summary>
    /// Whether this revision actually carries its value bytes (the effective
    /// outcome of <see cref="Mode"/> on this row): <see langword="true"/> when a
    /// value or CRDT delta preview is present, <see langword="false"/> when only
    /// a value hash and length were retained
    /// (<see cref="HistoryRetentionMode.MetadataOnly"/>, or an aged-out
    /// <see cref="HistoryRetentionMode.Hybrid"/> row). A delete or range-tombstone
    /// revision carries no value and reports <see langword="false"/>.
    /// </summary>
    [Id(1)] public bool ValueRetained { get; init; }
}
