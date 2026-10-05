using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The leaf's birth rule (issue #4654): a leaf with no state row writes a first
/// row, and serves data, only when it is being created.
/// <para>
/// The row is the leaf's only link to its tree, key range, projection checkpoint
/// and kept-snapshot record (issue #4634). If it vanishes - lost storage, or a row
/// deleted outside the lattice - while routing still names the leaf, the next
/// activation finds no row and no tree id and has nothing to replay from. It used
/// to serve an empty cache, so every key it held read as absent with no error, and
/// a call that bound it (the #1744 re-bind) turned it into an empty bound leaf for
/// good.
/// </para>
/// <para>
/// A rowless activation is either a leaf being created or a leaf whose row was
/// lost, and once its row record (below) is lost as well nothing on the leaf can
/// tell the two apart. So the leaf does not guess: every path that creates a leaf
/// (shard bootstrap, leaf split, bulk load, the recovery reseed of a purged tree)
/// carries a create intent naming it (<see cref="LatticeNewLeafIntentContext"/>),
/// and a rowless activation without one fails every data operation closed and
/// refuses to write a row.
/// </para>
/// <para>
/// The row record, a separate leaf-keyed row (<see cref="ILeafRowRecordGrain"/>),
/// is defence in depth. <c>PersistAsync</c> makes it durable before the first state
/// write, and <c>ClearGrainStateAsync</c> deletes it only after the row and the
/// snapshot. A create intent for a leaf whose record or snapshot survives is a
/// creator about to replace a lost row with an empty one, so it is refused too.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Set once this activation has been admitted as a leaf being created, so its
    /// first state write may land and its data operations are served.
    /// </summary>
    private bool _createIntentAdmitted;

    /// <summary>The row-record sidecar, or <see langword="null"/> for a leaf without a Guid key.</summary>
    private ILeafRowRecordGrain? RowRecord =>
        context.GrainId.TryGetGuidKey(out var leafKey, out _)
            ? grainFactory.GetGrain<ILeafRowRecordGrain>(leafKey)
            : null;

    /// <summary>
    /// Whether this activation has no state row, no tree id and no admission as a
    /// leaf being created, so it must not serve data: it may be a leaf whose row was
    /// lost (issue #4654). A deliberately cleared activation keeps its existing
    /// behaviour until it deactivates.
    /// </summary>
    private bool IsUnadmittedRowlessActivation =>
        !_createIntentAdmitted
        && !_leafStateCleared
        && !state.RecordExists
        && string.IsNullOrEmpty(state.State.TreeId);

    /// <summary>
    /// The data-operation gate of an unadmitted rowless activation: a caller the
    /// internal-origin guard refuses is refused first, so the refusal reveals
    /// nothing about the leaf; a call without a create intent naming this leaf
    /// fails closed; one with an intent admits the leaf as being created (unless a
    /// surviving row record or snapshot shows its row was lost) and proceeds.
    /// </summary>
    private async Task AwaitAdmissionThenReplayBarrierAsync()
    {
        EnsureInternalOrigin(LatticeOperation.Read);
        if (!LatticeNewLeafIntentContext.IsFor(context.GrainId))
        {
            throw new LeafStateRowLostException(context.GrainId.ToString(), treeId: null,
                "it was not reached through a path creating it, so its row may have been lost", innerException: null);
        }

        if (IsUnadmittedRowlessActivation)
        {
            await AdmitCreateIntentAsync();
        }

        await AwaitReplayBarrierAsync();
    }

    /// <summary>
    /// Admits a rowless activation as a leaf being created before its first state
    /// write, then makes the row record durable before any state write that would
    /// be the first durable trace of the row. A refusal or a failed record write
    /// fails the state write.
    /// </summary>
    private async Task AdmitAndRecordRowAsync()
    {
        if (!state.RecordExists && !_createIntentAdmitted)
        {
            await AdmitCreateIntentAsync();
        }

        if (state.State.RowRecorded || RowRecord is not { } record)
            return;

        await record.RecordAsync(state.State.TreeId);
        state.State.RowRecorded = true;
    }

    /// <summary>
    /// Admits this rowless activation as a leaf being created, or throws: the call
    /// must carry a create intent naming this leaf, and neither a row record nor a
    /// snapshot may survive from an earlier row. Either surviving means the row was
    /// written and lost, and creating the leaf anew would replace what it held with
    /// nothing.
    /// </summary>
    private async Task AdmitCreateIntentAsync()
    {
        if (!LatticeNewLeafIntentContext.IsFor(context.GrainId))
        {
            throw new LeafStateRowLostException(context.GrainId.ToString(), state.State.TreeId,
                "the write that would create it was not issued by a path creating it, so its row may have been lost",
                innerException: null);
        }

        if (context.GrainId.TryGetGuidKey(out var leafKey, out _) && RowRecord is { } record)
        {
            string? evidence = null;
            string? treeId = null;
            Exception? fault = null;
            try
            {
                if (await record.GetAsync() is { } recorded)
                {
                    evidence = "its row record shows a row was written";
                    treeId = recorded.TreeId;
                }
                else if (await grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey).LoadAsync(CancellationToken.None) is not null)
                {
                    evidence = "a snapshot of it survives, so a row was written";
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                evidence = "its row record or snapshot could not be read to tell a lost row from one never written";
                fault = ex;
            }

            if (evidence is not null && fault is null && !state.RecordExists)
            {
                // A duplicate activation that lost the create race of a leaf with a
                // deterministic id (a shard's root leaf, a bulk-load leaf) sees the
                // winner's record; the winner's row is then in storage, and the
                // first-create adopt converges on it (#1557). A record with no row
                // behind it is a lost row.
                await state.ReadStateAsync();
                if (state.RecordExists)
                {
                    _createIntentAdmitted = true;
                    return;
                }
            }

            if (evidence is not null && !state.RecordExists)
            {
                throw new LeafStateRowLostException(context.GrainId.ToString(), treeId ?? state.State.TreeId, evidence, fault);
            }
        }

        _createIntentAdmitted = true;
    }

    /// <summary>
    /// Deletes the row record once the leaf's row has been deliberately cleared.
    /// </summary>
    private Task ClearRowRecordAsync() =>
        RowRecord is { } record ? record.ClearAsync() : Task.CompletedTask;
}
