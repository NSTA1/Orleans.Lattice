namespace Orleans.Lattice.Replication;

/// <summary>
/// Fans one peer-status read out to a set of per-silo endpoints concurrently and
/// folds the answers with <see cref="ReplicationPeerStatusMerge"/>. Separated from
/// the grain-service client so the fan-out and fold are testable without a silo.
/// </summary>
internal static class ReplicationPeerStatusFanOut
{
    /// <summary>
    /// Issues <paramref name="request"/> to every target and merges the answers.
    /// A target that fails faults the whole read: a partial answer would present
    /// a silo's links as absent rather than unknown.
    /// </summary>
    /// <typeparam name="TTarget">The endpoint address type.</typeparam>
    /// <param name="targets">The endpoints to read. Must not be <see langword="null"/>.</param>
    /// <param name="read">Reads one endpoint. Must not be <see langword="null"/>.</param>
    /// <param name="request">The read to perform. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the wait for the answers.</param>
    /// <returns>The merged, ordered rows, at most the request's effective limit.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public static async Task<IReadOnlyList<ReplicationPeerStatusRow>> ReadAsync<TTarget>(
        IReadOnlyList<TTarget> targets,
        Func<TTarget, ReplicationPeerStatusReadRequest, Task<ReplicationPeerStatusRow[]>> read,
        ReplicationPeerStatusReadRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targets);
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        if (targets.Count == 0)
        {
            return Array.Empty<ReplicationPeerStatusRow>();
        }

        var pending = new Task<ReplicationPeerStatusRow[]>[targets.Count];
        for (var i = 0; i < targets.Count; i++)
        {
            pending[i] = read(targets[i], request);
        }

        var answers = await Task.WhenAll(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        return ReplicationPeerStatusMerge.Merge(answers, request.EffectiveLimit);
    }
}
