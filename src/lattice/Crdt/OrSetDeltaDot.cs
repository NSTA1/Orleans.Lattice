namespace Orleans.Lattice;

/// <summary>
/// A single entry in an observed-remove (OR) set delta: a unique
/// (replica id, counter) "dot" attached to an element. The dot context
/// allows concurrent adds and removes of the same element across
/// clusters to converge - a remove cancels exactly the dots it observed,
/// so a concurrent add on another replica with a different dot survives
/// the merge.
/// <para>
/// <strong>Equality caveat.</strong> The synthesized record-struct equality
/// operator delegates to the default comparer for each field, and the
/// default comparer for <see cref="byte"/><c>[]</c> is <em>reference</em>
/// equality. Two structurally-identical <c>OrSetDeltaDot</c> instances built
/// from independently-allocated <see cref="Element"/> arrays therefore
/// compare unequal. Consumers comparing dots across deltas (e.g. matching
/// an entry in <see cref="OrSetDelta.Removes"/> against the local set)
/// must compare <see cref="Element"/> by content, not via record equality
/// or <see cref="System.Linq.Enumerable.Contains{T}(System.Collections.Generic.IEnumerable{T},T)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrSetDeltaDot)]
[Immutable]
public readonly record struct OrSetDeltaDot
{
    /// <summary>The element bytes the dot is attached to. Never <c>null</c> on emitter-produced dots.</summary>
    [Id(0)] public byte[] Element { get; init; }

    /// <summary>
    /// The id of the replica that authored the dot. Combined with
    /// <see cref="Counter"/> this forms a globally-unique identifier for
    /// a single add operation.
    /// </summary>
    [Id(1)] public string ReplicaId { get; init; }

    /// <summary>
    /// The replica-local monotonic counter at the moment the dot was
    /// authored. Strictly greater than any prior counter from the same
    /// replica.
    /// </summary>
    [Id(2)] public long Counter { get; init; }
}
