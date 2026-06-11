using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.LatticeCrossTreeReceiverGrain"/>, the
/// receiver-side coordinator of a replicated cross-tree atomic write. One
/// activation per <c>(originClusterId, operationId)</c>. The coordinator is the
/// <b>single global decision authority on this receiver cluster</b>: every
/// participating tree's registry delegates its replicated sub-saga's status to
/// this grain, so the barrier-completing transition flips the cross-tree batch's
/// visibility on every replicated participating tree at one atomic moment, even
/// though each tree's terminals arrive independently over replication.
/// <para>
/// State is persisted before <c>NotifyTerminalAsync</c> returns so the
/// registration that precedes the notify is linearized against a durable
/// decision: a crash after the decision is recorded never loses it, and a
/// redelivered terminal re-heals materialization idempotently.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeReceiverState)]
internal sealed class CrossTreeReceiverState
{
    /// <summary>
    /// The frozen wait set: the participant tree-ids replicated on this
    /// receiver (<c>participants ∩ trees-replicated-here</c>), snapshotted from
    /// the first terminal's <see cref="CrossTreeReceiverTerminal.WaitSet"/> and
    /// validated for exact match on every later terminal. Empty until the first
    /// terminal arrives. Trees touched by the cross-tree batch but not
    /// replicated here are absent, so the barrier completes on the present
    /// subset (partial-replication batches are valid).
    /// </summary>
    [Id(0)] public List<string> WaitSet { get; set; } = [];

    /// <summary>
    /// Map from a participating tree-id to its arrived terminal. The barrier
    /// completes when <c>Arrived.Keys ⊇ WaitSet</c>; the global verdict is
    /// commit iff every arrived terminal voted commit. Carries the per-tree
    /// fan-out seeds (<see cref="CrossTreeReceiverTerminal.ObservedSourceShards"/>),
    /// terminal HLC and origin needed to build the finalize records.
    /// </summary>
    [Id(1)] public Dictionary<string, CrossTreeReceiverTerminal> Arrived { get; set; } = [];

    /// <summary><c>true</c> once the wait set is complete and the global decision is recorded.</summary>
    [Id(2)] public bool Decided { get; set; }

    /// <summary>The global verdict; meaningful only when <see cref="Decided"/> is <c>true</c>.</summary>
    [Id(3)] public bool Committed { get; set; }

    /// <summary>The source cluster id (first half of this grain's compound key).</summary>
    [Id(4)] public string OriginClusterId { get; set; } = string.Empty;

    /// <summary>The cross-tree operation id (second half of this grain's compound key).</summary>
    [Id(5)] public string OperationId { get; set; } = string.Empty;

    /// <summary>Wall-clock UTC tick stamped when the first terminal arrives; drives diagnostics.</summary>
    [Id(6)] public long StartedAtTicks { get; set; }
}
