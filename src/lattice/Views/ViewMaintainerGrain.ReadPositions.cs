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
    /// the interleaved <see cref="GetDurableReadPositionsAsync"/>. A
    /// <see langword="null"/> <see cref="ViewReadPositions.Positions"/> means the
    /// maintainer reads no log.
    /// </summary>
    private volatile ViewReadPositions? _readPositions;

    /// <summary>The source logs this activation has registered with.</summary>
    private readonly HashSet<string> _registeredReadLogs = new(StringComparer.Ordinal);

    private sealed record ViewReadPositions(string? WalTreeId, long[]? Positions);

    /// <inheritdoc />
    public Task<long[]?> GetDurableReadPositionsAsync(string walTreeId)
    {
        var snapshot = _readPositions;
        if (snapshot is null)
        {
            // Reached before the activation hook published: hold the whole log.
            return Task.FromResult<long[]?>([]);
        }

        if (snapshot.Positions is null
            || (snapshot.WalTreeId is { } bound && !string.Equals(bound, walTreeId, StringComparison.Ordinal)))
        {
            return Task.FromResult<long[]?>(null);
        }

        return Task.FromResult<long[]?>((long[])snapshot.Positions.Clone());
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

        await grainFactory.GetGrain<IWalOffsetConsumerRegistryGrain>(walTreeId).RegisterAsync(context.GrainId);
        _registeredReadLogs.Add(walTreeId);

        // Answer for this log from the resume point as it stands, which is never
        // ahead of what is persisted at the moment a read is about to start.
        _readPositions = new ViewReadPositions(walTreeId, ToPositions(state.State.AppliedOffsets));
    }

    /// <summary>
    /// Publishes <paramref name="offsets"/> as this view's read positions in
    /// <paramref name="walTreeId"/>. Called after the write that persisted them,
    /// or with a rebuild's captured resume floors.
    /// </summary>
    private void PublishReadPositions(string walTreeId, IReadOnlyDictionary<int, long> offsets)
        => _readPositions = new ViewReadPositions(walTreeId, ToPositions(offsets));

    /// <summary>
    /// Lowers the published positions for <paramref name="walTreeId"/> to
    /// <paramref name="offsets"/> wherever that is lower, before a write that
    /// may persist them.
    /// </summary>
    private void LowerReadPositions(string walTreeId, IReadOnlyDictionary<int, long> offsets)
    {
        var current = _readPositions;
        var candidate = ToPositions(offsets);
        if (current?.Positions is not { } published
            || (current.WalTreeId is { } bound && !string.Equals(bound, walTreeId, StringComparison.Ordinal)))
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

        _readPositions = new ViewReadPositions(walTreeId, lowered);
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
