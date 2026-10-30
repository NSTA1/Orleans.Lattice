namespace Orleans.Lattice.Views;

/// <summary>
/// The read-your-writes source-head barrier (<see cref="IViewMaintainerGrain.WaitForSourceHeadAsync"/>).
/// </summary>
/// <remarks>
/// The barrier is positional, not an HLC watermark (#5308). A source write can carry
/// an HLC below what the view has already applied - <see cref="ILattice.MergeAsync(string, CancellationToken)"/>
/// keeps the peer's HLC - and a cutover onto a new physical copy restarts the source
/// log, so comparing the view's highest applied HLC against the source's head HLC
/// can declare a write visible before the view has read it. Instead the barrier
/// captures, at the call, the source's current physical identity and the head offset
/// of each of its WAL partitions, and returns only once the view is bound to that
/// identity and has consumed every entry below each captured head. The cost is paid
/// on the barrier path only; the drain is unchanged.
/// </remarks>
internal sealed partial class ViewMaintainerGrain
{
    private async Task WaitForSourceHeadPositionsAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            Options,
            state.State.ActiveGeneration);
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            // Nothing is maintained, so there is nothing to wait for.
            return;
        }

        await ThrowIfShipViewConsumerAsync(registration, cancellationToken);

        var deadline = DateTime.UtcNow + timeout;
        var physical = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
        var heads = await SampleSourceHeadsAsync(physical, cancellationToken);
        while (true)
        {
            if (ReflectsSourceHead(physical, heads))
            {
                return;
            }

            // The binding names a different physical copy than the source resolves
            // to now: ask the drain to re-resolve and heal, exactly as an alias-swap
            // push would, rather than wait for the push or the backstop.
            if (!IsBoundToPhysical(physical))
            {
                _sourceIdentityRebindRequests++;
            }

            await DrainAsync(cancellationToken);

            if (ReflectsSourceHead(physical, heads))
            {
                return;
            }

            // Still bound elsewhere after a drain. If the source moved again since
            // the call, the writes it holds are those of its newest copy, so follow
            // it: wait on that copy's identity and heads instead.
            if (!IsBoundToPhysical(physical))
            {
                var current = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
                if (!string.Equals(current, physical, StringComparison.Ordinal))
                {
                    physical = current;
                    heads = await SampleSourceHeadsAsync(physical, cancellationToken);
                }
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"View '{ViewName}' did not reflect the head of source physical tree '{physical}' within {timeout}.");
            }

            var remaining = deadline - DateTime.UtcNow;
            var delay = remaining < PollInterval ? remaining : PollInterval;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private async Task<long[]> SampleSourceHeadsAsync(string physical, CancellationToken cancellationToken) =>
        await SampleHeadOffsetsAsync(physical, await optionsResolver.GetWalPartitionsAsync(physical), cancellationToken);

    private bool IsBoundToPhysical(string physical) =>
        string.Equals(state.State.BoundPhysicalTreeId, physical, StringComparison.Ordinal);

    /// <summary>
    /// Whether the view reflects every committed entry below <paramref name="heads"/>
    /// on <paramref name="physical"/>: it is bound to that copy, it has consumed each
    /// partition through its captured head, and no batch whose commit it has read is
    /// still waiting to be published.
    /// </summary>
    /// <remarks>
    /// A partition's consumed position is its durable checkpoint, raised to the read
    /// position the drain holds in memory while an atomic batch is staged (the
    /// checkpoint is held back below the oldest staged prepare so a restart re-reads
    /// it). A staged batch without a commit terminal is not a committed write and does
    /// not hold the barrier; one whose commit was read but which has not been flushed
    /// yet does.
    /// </remarks>
    private bool ReflectsSourceHead(string physical, long[] heads)
    {
        if (!IsBoundToPhysical(physical))
        {
            return false;
        }

        var stagedReadsApply = _staging.Count != 0
            && string.Equals(_stagingReadWalTreeId, physical, StringComparison.Ordinal);
        for (var partition = 0; partition < heads.Length; partition++)
        {
            var consumed = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            if (stagedReadsApply && _stagingReadOffsets.TryGetValue(partition, out var read) && read > consumed)
            {
                consumed = read;
            }

            if (consumed < heads[partition] - 1)
            {
                return false;
            }
        }

        foreach (var tx in _staging.Values)
        {
            if (tx.TerminalSeen)
            {
                return false;
            }
        }

        return true;
    }
}
