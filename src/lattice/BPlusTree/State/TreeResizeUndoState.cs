using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent undo-intent slot for <see cref="Grains.TreeResizeGrain"/>, stored
/// in its own storage row beside <see cref="TreeResizeState"/>.
/// <para>
/// The slot is separate so the interleaved undo request can persist the
/// operator's intent while a resize phase holds the coordinator's turn, without
/// writing the phase state that turn is mutating: two writers on one row would
/// race each other's ETag and could persist a half-applied phase transition
/// (issue 3923). Every write to this slot is serialised by the coordinator's
/// undo-intent gate, and nothing here is coordinator phase state - the phase
/// loop only reads it to decide whether to unwind.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeResizeUndoState)]
internal sealed class TreeResizeUndoState
{
    /// <summary>
    /// The <see cref="TreeResizeState.OperationId"/> of the resize an operator
    /// asked to undo, or <see langword="null"/> when no undo has been requested.
    /// An intent is pending only while it still names the coordinator's current
    /// resize and that resize can be undone; once the unwind resets the resize
    /// state, or a new resize replaces the operation id, the intent is inert.
    /// </summary>
    [Id(0)] public string? RequestedOperationId { get; set; }

    /// <summary>When the pending undo was accepted, in UTC.</summary>
    [Id(1)] public DateTime? RequestedAtUtc { get; set; }

    /// <summary>The operation id of the resize most recently undone.</summary>
    [Id(2)] public string? UndoneOperationId { get; set; }

    /// <summary>When the resize named by <see cref="UndoneOperationId"/> finished unwinding, in UTC.</summary>
    [Id(3)] public DateTime? UndoneAtUtc { get; set; }

    /// <summary>
    /// The operation id of a resize whose accepted undo could not be applied and
    /// was withdrawn, or <see langword="null"/>. Cleared when a new undo is accepted.
    /// </summary>
    [Id(4)] public string? FailedOperationId { get; set; }

    /// <summary>The reason the undo named by <see cref="FailedOperationId"/> was withdrawn.</summary>
    [Id(5)] public string? FailureMessage { get; set; }
}
