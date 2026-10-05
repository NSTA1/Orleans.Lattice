namespace Orleans.Lattice.Replication;

/// <summary>
/// The <see cref="ITreeLineageSource"/> for a tree registry that stamps no
/// lineage: every tree reports none, so every receiver tree frontier stays in
/// degraded mode - sound, with only an applied write's exact identity meeting a
/// dependency on it.
/// </summary>
internal sealed class UntrackedTreeLineageSource : ITreeLineageSource
{
    /// <inheritdoc />
    public Task<Guid?> GetLineageAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<Guid?>(null);
    }
}