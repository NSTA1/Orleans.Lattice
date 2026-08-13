namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Request to delete every authorized key in a bounded half-open key range
/// <c>[<see cref="StartInclusive"/>, <see cref="EndExclusive"/>)</c> on a single
/// tree. The facade drains a durable range-delete cursor to completion with
/// transparent reconnect on transient enumerator loss, so the whole range is
/// tombstoned in bounded batches rather than one unbounded call.
/// </summary>
/// <remarks>
/// Both bounds are <b>required</b>: a range delete is a bulk mutation, so an
/// open-ended range is refused up front rather than risking an unbounded delete.
/// The delete is pruned to nothing for an unauthorized (or anonymous) caller by
/// the gated <see cref="ILattice"/> surface, which enforces the range delete
/// all-or-nothing across its whole span.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataRangeDeleteRequest)]
[Immutable]
public sealed record DataRangeDeleteRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Inclusive lower key bound. Required (non-null).</summary>
    [Id(1)] public required string StartInclusive { get; init; }

    /// <summary>Exclusive upper key bound. Required (non-null).</summary>
    [Id(2)] public required string EndExclusive { get; init; }
}
