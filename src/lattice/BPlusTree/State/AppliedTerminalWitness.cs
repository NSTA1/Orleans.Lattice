namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One leaf's durable record that a saga's terminal settled a set of keys on it
/// (issue #4545): the keys whose prepared bucket the terminal drained or
/// discarded here, and the keys its committed-values backstop installed here.
/// <para>
/// A destination-side shadow marker for a settled key guards nothing, because
/// the row already incorporates the saga, and the terminal that would clear it
/// has come and gone. The record is what lets the leaf recognise such a marker
/// - refusing to install it, declining to carry it across a split, and serving
/// the key past it - after a reactivation has dropped the activation-scoped
/// terminal memory and on a split sibling that never saw the terminal at all.
/// </para>
/// <para>
/// It is persisted in <see cref="LeafNodeState"/>, so it is written atomically
/// with every projection checkpoint and therefore records every terminal the
/// checkpoint has scanned past; replay rebuilds the rest. It is scoped per key,
/// not per saga: one saga's terminal can reach a leaf for some keys while its
/// committed value for another key is still on its way.
/// </para>
/// </summary>
/// <param name="TransactionId">The saga whose terminal settled the keys.</param>
/// <param name="Keys">The keys the terminal settled on this leaf.</param>
/// <param name="RecordedAtTicks">
/// UTC ticks when the record was first made, so a prune pass only asks the
/// registry about records old enough to have possibly been forgotten.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.AppliedTerminalWitness)]
[Immutable]
internal readonly record struct AppliedTerminalWitness(
    [property: Id(0)] Guid TransactionId,
    [property: Id(1)] string[] Keys,
    [property: Id(2)] long RecordedAtTicks);
