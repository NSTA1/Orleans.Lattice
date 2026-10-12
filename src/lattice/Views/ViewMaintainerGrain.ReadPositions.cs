using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Views;

/// <summary>
/// The view maintainer as an <see cref="IWalOffsetConsumer"/> (issue #4584): it
/// tells the WAL GC on every silo the lowest offset per partition of its source
/// log that it has not durably consumed. Before this, its progress reached the GC
/// only as an HLC cursor in the process-local cursor registry, which a GC pass on
/// any other silo cannot see, and which is not an offset bound in any case.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is published.</b> The durable resume point, <c>AppliedOffsets[p] + 1</c>
/// (already held back below every staged atomic batch). It is lowered before a
/// write that lowers it and raised only after the write that made it durable. A
/// rebuild publishes the resume floors it captures before it scans the source,
/// so the entries appended during the scan are held. That is safe without a
/// write because a rebuild that does not complete leaves a state from which the
/// view rebuilds again or detects the trimmed gap as a fall-off.
/// </para>
/// <para>
/// <b>Registration.</b> The maintainer registers with a log's
/// <see cref="IWalOffsetConsumerRegistryGrain"/> before it reads that log. After
/// an alias swap it is registered with the new log before it withdraws from the
/// old one, and ShipView suppression and decommission withdraw it.
/// </para>
/// </remarks>
internal sealed partial class ViewMaintainerGrain : IWalOffsetConsumer
{
    /// <summary>
    /// The published read positions: an immutable snapshot replaced whole, read by
    /// the interleaved <see cref="GetDurableReadPositionsAsync"/>. A rebuild holds
    /// both its candidate log and the old bound log until durable publication. A
    /// <see langword="null"/> <see cref="ViewReadPositions.Positions"/> means the
    /// maintainer reads no log.
    /// </summary>
    private volatile ViewReadPositions? _readPositions;

    /// <summary>The source logs this activation has registered with.</summary>
    private readonly HashSet<string> _registeredReadLogs = new(StringComparer.Ordinal);

    private sealed record ViewReadPositions(
        string? WalTreeId,
        long[]? Positions,
        string? RetainedWalTreeId = null,
        long[]? RetainedPositions = null);

    /// <inheritdoc />
    public Task<long[]?> GetDurableReadPositionsAsync(string walTreeId)
    {
        var snapshot = _readPositions;
        if (snapshot is null)
        {
            // Reached before the activation hook published: hold the whole log.
            return Task.FromResult<long[]?>([]);
        }

        var positions = snapshot.WalTreeId is { } bound && !string.Equals(bound, walTreeId, StringComparison.Ordinal)
            ? string.Equals(snapshot.RetainedWalTreeId, walTreeId, StringComparison.Ordinal) ? snapshot.RetainedPositions : null
            : snapshot.Positions;
        if (positions is null)
        {
            return Task.FromResult<long[]?>(null);
        }

        return Task.FromResult<long[]?>((long[])positions.Clone());
    }

    /// <summary>
    /// Publishes the persisted resume point for the persisted bound log. A view
    /// that has never bound a log has never registered with one, so it answers
    /// for none until a drain registers it.
    /// </summary>
    private void PublishActivationReadPositions()
    {
        var bound = state.State.BoundPhysicalTreeId;
        _readPositions = string.IsNullOrEmpty(bound)
            ? new ViewReadPositions(null, null)
            : new ViewReadPositions(bound, ToPositions(state.State.AppliedOffsets));
    }

    /// <summary>
    /// Registers this maintainer as an offset consumer of <paramref name="walTreeId"/>
    /// before it reads that log, once per activation.
    /// </summary>
    private async Task EnsureReadRegisteredAsync(string walTreeId)
    {
        if (_registeredReadLogs.Contains(walTreeId))
        {
            return;
        }

        // Registration can become visible before its response. Publish the hold
        // first; an old physical log's offsets cannot bound the candidate log.
        var positions = string.Equals(state.State.BoundPhysicalTreeId, walTreeId, StringComparison.Ordinal)
            ? ToPositions(state.State.AppliedOffsets)
            : [];
        string? retainedLog = null;
        long[]? retainedPositions = null;
        var previous = _readPositions;
        var persistedLog = previous?.RetainedWalTreeId ?? state.State.BoundPhysicalTreeId;
        if (persistedLog is { Length: > 0 }
            && !string.Equals(persistedLog, walTreeId, StringComparison.Ordinal))
        {
            retainedLog = persistedLog;
            retainedPositions = string.Equals(previous?.WalTreeId, persistedLog, StringComparison.Ordinal)
                ? previous!.Positions
                : string.Equals(previous?.RetainedWalTreeId, persistedLog, StringComparison.Ordinal)
                    ? previous!.RetainedPositions
                    : ToPositions(state.State.AppliedOffsets);
        }

        _readPositions = new ViewReadPositions(walTreeId, positions, retainedLog, retainedPositions);
        await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(walTreeId).RegisterAsync(context.GrainId);
        _registeredReadLogs.Add(walTreeId);
    }

    /// <summary>
    /// Publishes <paramref name="offsets"/> as this view's read positions in
    /// <paramref name="walTreeId"/>. Called after the write that persisted them,
    /// or with a rebuild's captured resume floors. Only a successful tuple
    /// publication retires the previous bound log.
    /// </summary>
    private void PublishReadPositions(string walTreeId, IReadOnlyDictionary<int, long> offsets, bool retirePrevious = false)
    {
        var previous = _readPositions;
        if (!retirePrevious && string.Equals(previous?.RetainedWalTreeId, walTreeId, StringComparison.Ordinal))
        {
            _readPositions = previous! with { RetainedPositions = ToPositions(offsets) };
            return;
        }

        _readPositions = new ViewReadPositions(walTreeId, ToPositions(offsets),
            retirePrevious ? null : previous?.RetainedWalTreeId,
            retirePrevious ? null : previous?.RetainedPositions);
    }

    /// <summary>
    /// Lowers the published positions for <paramref name="walTreeId"/> to
    /// <paramref name="offsets"/> wherever that is lower, before a write that
    /// may persist them.
    /// </summary>
    private void LowerReadPositions(string walTreeId, IReadOnlyDictionary<int, long> offsets)
    {
        var current = _readPositions;
        var candidate = ToPositions(offsets);
        var retained = string.Equals(current?.RetainedWalTreeId, walTreeId, StringComparison.Ordinal);
        var published = retained ? current?.RetainedPositions
            : current?.WalTreeId is { } bound && !string.Equals(bound, walTreeId, StringComparison.Ordinal)
                ? null : current?.Positions;
        if (published is null)
        {
            return;
        }

        var length = Math.Max(published.Length, candidate.Length);
        var lowered = new long[length];
        for (var p = 0; p < length; p++)
        {
            lowered[p] = Math.Min(
                p < published.Length ? published[p] : 0,
                p < candidate.Length ? candidate[p] : 0);
        }

        _readPositions = retained
            ? current! with { RetainedPositions = lowered }
            : current! with { Positions = lowered };
    }

    /// <summary>
    /// Withdraws this maintainer from <paramref name="walTreeId"/>'s offset
    /// consumers. Best-effort: a stale registration holds nothing, because the
    /// maintainer answers <see langword="null"/> for a log it no longer reads.
    /// </summary>
    private async Task WithdrawReadRegistrationAsync(string walTreeId)
    {
        _registeredReadLogs.Remove(walTreeId);
        try
        {
            await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(walTreeId).UnregisterAsync(context.GrainId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "View '{ViewName}' failed to withdraw from the offset consumers of log '{Log}'; it no longer holds that log.",
                ViewName, walTreeId);
        }
    }

    /// <summary>Publishes that this maintainer reads no log, so it holds nothing.</summary>
    private void PublishNoReadPositions() => _readPositions = new ViewReadPositions(null, null);

    /// <summary>
    /// The first offset per partition not yet consumed, from the last-read
    /// offsets (<c>-1</c> or absent for a partition nothing has been read from).
    /// </summary>
    private static long[] ToPositions(IReadOnlyDictionary<int, long> offsets)
    {
        var length = 0;
        foreach (var partition in offsets.Keys)
        {
            length = Math.Max(length, partition + 1);
        }

        var positions = new long[length];
        foreach (var (partition, offset) in offsets)
        {
            if (partition >= 0)
            {
                positions[partition] = Math.Max(0, offset + 1);
            }
        }

        return positions;
    }
}
