using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One durably recorded piece of replay work that the incremental flush
/// ceiling was allowed to advance past (issue #2165).
/// <para>
/// Two classes of WAL record used to pin a partition's flush ceiling for the
/// whole of an activation, because neither survives a teardown as in-memory
/// state and both therefore had to be re-read from the WAL by the next
/// activation:
/// </para>
/// <list type="number">
/// <item>An <b>unresolved saga prepare</b>. Applying it populates the
/// activation-scoped <c>_pendingTx</c> bucket only, so the checkpoint could
/// not pass it without a resumed replay losing the prepared write when its
/// terminal later arrived.</item>
/// <item>An <b>undrained deferred terminal</b> (<c>TxCommit</c> /
/// <c>TxAbort</c> / <c>DeleteRange</c>) on a partition that pass 1 does not
/// absorb last. Its mutation is genuinely not applied until pass 2, so the
/// checkpoint could not pass it at all.</item>
/// </list>
/// <para>
/// Recording the record here - persisted atomically with the checkpoint it
/// licenses, because both live in the same <see cref="LeafNodeState"/> row -
/// removes the need to re-read it, which is precisely what the pass-1 sweep
/// comment in <c>BPlusLeafGrain.Activation.cs</c> named as the requirement for
/// removing the residual livelock #2089 left behind. A resumed replay
/// reconstructs the work from this list before it reads a single WAL slice.
/// </para>
/// </summary>
/// <param name="Partition">
/// The WAL partition whose offset space <paramref name="Offset"/> belongs to.
/// Offsets are only comparable within a partition, so the pairing is
/// load-bearing for the per-partition ceiling clamps.
/// </param>
/// <param name="Offset">
/// The WAL offset the mutation was read from. Used to strike the entry off
/// once the work resolves, and to keep reconstruction in WAL order.
/// </param>
/// <param name="Mutation">
/// The mutation verbatim, so reconstruction goes through the identical
/// <c>ILeafProjection.Apply</c> path a re-read would have taken.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.UnresolvedReplayWorkEntry)]
[Immutable]
internal readonly record struct UnresolvedReplayWorkEntry(
    [property: Id(0)] int Partition,
    [property: Id(1)] long Offset,
    [property: Id(2)] LatticeMutation Mutation);
