namespace Orleans.Lattice.Vector.Persistence;

public sealed partial class DurableVectorIndex
{
    // The generation a trained layout is being committed under, or -1 when no
    // commit is in flight. It is chosen once, on the first attempt, and every
    // retry reuses it: recomputing it from the committed generation is what let
    // a retry after a committed write start yet another generation (#3547).
    private long _commitTarget = -1;

    // The lowest generation the in-flight commit supersedes, or -1 when none is
    // awaiting deletion. Every generation from here up to the target is deleted
    // once the target is committed, and the field moves past each one as its
    // delete lands, so a failed delete resumes where it stopped.
    private long _supersededFrom = -1;

    /// <summary>
    /// Commits the trained layout as a new generation, then deletes the
    /// generation it supersedes. Safe to call again after any failure.
    /// <para>
    /// The two halves are separate steps with their own recorded progress. A
    /// failure while writing leaves the target generation and the partitions
    /// already committed under it recorded, so the retry writes only the rest
    /// rather than the whole index again. A failure while deleting leaves the
    /// target committed, so the retry only deletes: it never writes another
    /// generation, and the superseded one is never forgotten and orphaned.
    /// </para>
    /// </summary>
    private async Task CommitTrainedGenerationAsync(CancellationToken cancellationToken)
    {
        if (_commitTarget < 0)
        {
            _commitTarget = _generation + 1;
            _supersededFrom = _generation;
        }

        if (_generation != _commitTarget)
        {
            await WritePartitionsAsync(_commitTarget, full: true, cancellationToken).ConfigureAwait(false);
        }

        await DeleteSupersededGenerationsAsync(cancellationToken).ConfigureAwait(false);
        _commitTarget = -1;
    }

    /// <summary>
    /// Deletes every generation the committed target supersedes, recording each
    /// delete as it lands.
    /// </summary>
    private async Task DeleteSupersededGenerationsAsync(CancellationToken cancellationToken)
    {
        while (_supersededFrom >= 0 && _supersededFrom < _commitTarget)
        {
            await _store.DeletePrefixAsync(
                VectorIndexStorageKeys.GenerationPrefix(_prefix, _supersededFrom), cancellationToken)
                .ConfigureAwait(false);
            _supersededFrom++;
        }

        _supersededFrom = -1;
    }

    /// <summary>
    /// Adopts the cleanup a committed generation left undone before a restart.
    /// <para>
    /// A build-state record names the generation it was written under, and it is
    /// deleted only once the generations a commit superseded have been. One that
    /// names a generation below the committed manifest's is therefore the durable
    /// trace of a commit whose write landed and whose cleanup did not: every
    /// generation from the recorded one up to the committed one is superseded
    /// derived data that nothing will ever read. Resuming at
    /// <see cref="VectorIndexBuildPhase.Persisting"/> with the committed
    /// generation as the target makes the next build step delete them and write
    /// nothing, rather than leaving them orphaned.
    /// </para>
    /// </summary>
    private void AdoptUnfinishedCommit(VectorIndexBuildState build, long committedGeneration)
    {
        _commitTarget = committedGeneration;
        _supersededFrom = build.Generation;
        _phase = VectorIndexBuildPhase.Persisting;
    }

    /// <summary>
    /// Whether a trained generation is part-way through being committed: written
    /// partly, or written and awaiting the deletion of what it supersedes.
    /// </summary>
    private bool CommitInFlight => _commitTarget >= 0;

    /// <summary>
    /// Forgets what a partial write committed under the target generation, so the
    /// next write of it starts again from the first partition. The target itself
    /// is kept: it is still the generation the commit will land under.
    /// </summary>
    private void AbandonPartialWrite()
    {
        _writingGeneration = -1;
        _writingCentroidEpoch = -1;
    }

    /// <summary>
    /// Forgets any commit in flight, for a reset of the in-memory state.
    /// </summary>
    private void ResetCommitState()
    {
        _commitTarget = -1;
        _supersededFrom = -1;
        AbandonPartialWrite();
    }
}
