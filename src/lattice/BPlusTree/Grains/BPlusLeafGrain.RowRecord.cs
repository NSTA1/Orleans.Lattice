using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The leaf's row record (issue #4654): durable proof, kept outside the leaf's
/// own state row, that the row was once written.
/// <para>
/// The row is the leaf's only link to its tree, key range, projection checkpoint
/// and kept-snapshot record (issue #4634). If it vanishes - lost storage, or a row
/// deleted outside the lattice - the next activation finds no row and no tree id,
/// has nothing to replay from, and used to serve an empty cache, so every key it
/// held read as absent with no error. A missing row is also every leaf's first
/// activation, so the leaf needs evidence from somewhere the loss did not reach.
/// </para>
/// <para>
/// <c>PersistAsync</c> writes the record before the first state write of a leaf
/// (and before the first write after this change of a leaf that predates it), so
/// no row is ever durable without it. <c>ClearGrainStateAsync</c> deletes it only
/// after the row, alongside the snapshot, so a deliberate clear leaves no record
/// behind once it completes and an interrupted one stays failed closed until its
/// retry does. A rowless, unbound activation that finds the record, or a snapshot
/// that outlived the row, fails its replay closed.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Set once this activation has begun writing its own row, so the lost-row
    /// check does not mistake a row this activation is creating for a lost one.
    /// </summary>
    private bool _rowWriteAttemptedThisActivation;

    /// <summary>The row-record sidecar, or <see langword="null"/> for a leaf without a Guid key.</summary>
    private ILeafRowRecordGrain? RowRecord =>
        context.GrainId.TryGetGuidKey(out var leafKey, out _)
            ? grainFactory.GetGrain<ILeafRowRecordGrain>(leafKey)
            : null;

    /// <summary>
    /// Makes the row record durable before a state write that would otherwise be
    /// the first durable trace of this leaf's row. Runs at most once per leaf
    /// lifetime: the flag it sets rides the write that follows. A failure fails
    /// that write.
    /// </summary>
    private async Task EnsureRowRecordedAsync()
    {
        _rowWriteAttemptedThisActivation = true;
        if (state.State.RowRecorded || RowRecord is not { } record)
            return;

        await record.RecordAsync(state.State.TreeId);
        state.State.RowRecorded = true;
    }

    /// <summary>
    /// For an activation that found no state row and no tree id: the fault to fail
    /// its replay with when the row was once written (its row record is present, or
    /// a snapshot outlived it), or <see langword="null"/> when this is a leaf whose
    /// row was never written or was deliberately cleared. A record or snapshot that
    /// cannot be read fails closed.
    /// </summary>
    private async Task<Exception?> DetectLostStateRowAsync(CancellationToken cancellationToken)
    {
        if (state.RecordExists
            || !string.IsNullOrEmpty(state.State.TreeId)
            || _leafStateCleared
            || _rowWriteAttemptedThisActivation
            || RowRecord is not { } record
            || !context.GrainId.TryGetGuidKey(out var leafKey, out _))
        {
            return null;
        }

        string? evidence = null;
        string? treeId = null;
        Exception? fault = null;
        try
        {
            if (await record.GetAsync() is { } recorded)
            {
                evidence = "its row record shows the row was written";
                treeId = recorded.TreeId;
            }
            else if (await grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey).LoadAsync(cancellationToken) is not null)
            {
                evidence = "a snapshot of it survives, so the row was written";
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            evidence = "its row record or snapshot could not be read to tell a lost row from one never written";
            fault = ex;
        }

        // A birth seam that interleaved while the evidence was read may have
        // written the row; that row is being created here, not lost.
        if (evidence is null
            || state.RecordExists
            || !string.IsNullOrEmpty(state.State.TreeId)
            || _leafStateCleared
            || _rowWriteAttemptedThisActivation)
        {
            return null;
        }

        return new LeafStateRowLostException(context.GrainId.ToString(), treeId, evidence, fault);
    }

    /// <summary>
    /// Deletes the row record once the leaf's row has been deliberately cleared.
    /// </summary>
    private Task ClearRowRecordAsync() =>
        RowRecord is { } record ? record.ClearAsync() : Task.CompletedTask;
}
