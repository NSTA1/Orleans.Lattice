using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationTreeFrontierGrain"/>. See the interface for
/// the contract.
/// </summary>
internal sealed class ReplicationTreeFrontierGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ITreeLineageSource lineageSource,
    ILogger<ReplicationTreeFrontierGrain> logger,
    [PersistentState("replication-tree-frontier", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationTreeFrontierState> state)
    : IReplicationTreeFrontierGrain, IGrainBase
{
    /// <summary>
    /// The longest a raised per-origin watermark stays unpersisted while pushes
    /// keep arriving. A lagging stored watermark only delays after a restart.
    /// </summary>
    internal static readonly TimeSpan WatermarkPersistInterval = TimeSpan.FromSeconds(5);

    // The last aggregate forwarded to each origin's frontier, so a push forwards
    // only when the origin's aggregate moved.
    private readonly Dictionary<string, (long Generation, HybridLogicalClock LowWatermark)> _forwarded = new(StringComparer.Ordinal);

    // Origins whose cap this activation has confirmed lifted. A re-stamp whose
    // own write failed can leave a cap on the origin's frontier that the
    // reloaded state does not know about; the first accepted watermark per
    // activation lifts it, which is sound because a failed re-stamp means the
    // contents were not replaced.
    private readonly HashSet<string> _capConfirmed = new(StringComparer.Ordinal);

    // The mode each origin currently contributes to the frontier-origins gauge.
    private readonly Dictionary<string, string> _published = new(StringComparer.Ordinal);

    private bool _settledThisActivation;
    private bool _watermarksDirty;
    private long _persistedAt = Environment.TickCount64;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString() ?? string.Empty;

    /// <inheritdoc />
    public async Task<Guid> ObserveAsync(string originClusterId, ReplicationSourceFrontier? shipped, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        await EnsureSettledAsync(cancellationToken).ConfigureAwait(true);
        var epoch = state.State.Epoch;
        if (!state.State.Origins.TryGetValue(originClusterId, out var entry))
        {
            // An origin first seen in this epoch has nothing in the tree a
            // replacement could have lost, so it starts uncapped. Durable at
            // once: a forgotten origin would vanish from the reap gate's view.
            entry = new ReplicationTreeOriginFrontier();
            state.State.Origins[originClusterId] = entry;
            await WriteStateAsync().ConfigureAwait(true);
        }

        if (shipped is not { } frontier
            || epoch == Guid.Empty
            || frontier.ReceiverLineage != epoch
            || entry.AwaitingPin)
        {
            PublishModes();
            return epoch;
        }

        var origin = grainFactory.GetGrain<IReplicationOriginFrontierGrain>(originClusterId);
        if (frontier.AckedPositions is { } acked && acked != entry.AckedPositions)
        {
            entry.AckedPositions = acked;
            _watermarksDirty = true;
        }

        if (frontier.TreeLowWatermark > entry.LowWatermark)
        {
            entry.LowWatermark = frontier.TreeLowWatermark;
            _watermarksDirty = true;
            if (state.State.ExportHeldOrigins.Contains(originClusterId))
            {
                await DropExportHeldBelowAsync(origin, originClusterId, entry.LowWatermark, cancellationToken).ConfigureAwait(true);
            }
        }

        if (entry.Capped || !_capConfirmed.Contains(originClusterId))
        {
            // The origin re-covered the tree in this epoch: its aggregates from
            // this generation on count the tree's new coverage, not the old.
            await origin.LiftTreeCapAsync(TreeId, frontier.OriginGeneration, cancellationToken).ConfigureAwait(true);
            _capConfirmed.Add(originClusterId);
            if (entry.Capped)
            {
                entry.Capped = false;
                await WriteStateAsync().ConfigureAwait(true);
            }
        }

        if (!_forwarded.TryGetValue(originClusterId, out var last)
            || frontier.OriginGeneration > last.Generation
            || (frontier.OriginGeneration == last.Generation && frontier.OriginLowWatermark > last.LowWatermark))
        {
            await origin.RecordLowWatermarkAsync(frontier.OriginLowWatermark, frontier.OriginGeneration, cancellationToken).ConfigureAwait(true);
            _forwarded[originClusterId] = (frontier.OriginGeneration, frontier.OriginLowWatermark);
        }

        await MaybePersistWatermarksAsync().ConfigureAwait(true);
        PublishModes();
        return epoch;
    }

    /// <inheritdoc />
    public async Task OnContentsReplacingAsync(
        CancellationToken cancellationToken,
        bool preserveBootstrapFloor = false)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await RestampAsync(Guid.NewGuid(), cancellationToken, preserveBootstrapFloor).ConfigureAwait(true);
        state.State.Unsettled = true;
        await WriteStateAsync().ConfigureAwait(true);
        PublishModes();
    }

    /// <inheritdoc />
    public async Task OnLineageChangingAsync(Guid? nextLineage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await RestampAsync(nextLineage is null ? Guid.Empty : Guid.NewGuid(), cancellationToken, preserveBootstrapFloor: false).ConfigureAwait(true);
        state.State.ObservedRegistryLineage = nextLineage;
        state.State.Unsettled = false;
        _settledThisActivation = true;
        await WriteStateAsync().ConfigureAwait(true);
        PublishModes();
    }

    /// <inheritdoc />
    public async Task<bool> PinAsync(
        Guid epoch,
        IReadOnlyDictionary<string, HybridLogicalClock> sourceLowWatermarks,
        IReadOnlyDictionary<string, HybridLogicalClock[]> sourceHeld,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sourceLowWatermarks);
        ArgumentNullException.ThrowIfNull(sourceHeld);
        cancellationToken.ThrowIfCancellationRequested();

        await EnsureSettledAsync(cancellationToken).ConfigureAwait(true);
        if (epoch != state.State.Epoch || epoch == Guid.Empty)
        {
            // The contents were replaced while the bootstrap ran, or the tree is
            // degraded: the export does not describe what the tree holds now.
            return false;
        }

        // The source's held writes are not in the export, so they are held here
        // until applied. Published before any watermark that would pass them,
        // and replacing what the previous export published, so an origin the
        // new export holds nothing for is cleared.
        var heldOrigins = new HashSet<string>(state.State.ExportHeldOrigins, StringComparer.Ordinal);
        heldOrigins.UnionWith(sourceHeld.Keys);
        var stillHeld = new HashSet<string>(StringComparer.Ordinal);
        foreach (var origin in heldOrigins)
        {
            var held = sourceHeld.TryGetValue(origin, out var identities) ? identities : [];
            await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(origin)
                .SetHeldAsync(ReplicationOriginFrontierGrain.ExportSource(TreeId), held, cancellationToken)
                .ConfigureAwait(true);
            if (held.Length > 0)
            {
                stillHeld.Add(origin);
            }
        }

        state.State.ExportHeldOrigins = stillHeld;

        // Every origin is installed, not only those the export lists: the tree
        // now reflects the source's contents, and an origin the export carries
        // no watermark for starts again from zero, which ends the re-seed it was
        // awaiting.
        var origins = new HashSet<string>(state.State.Origins.Keys, StringComparer.Ordinal);
        origins.UnionWith(sourceLowWatermarks.Keys);
        foreach (var origin in origins)
        {
            var lowWatermark = sourceLowWatermarks.TryGetValue(origin, out var exported) ? exported : HybridLogicalClock.Zero;
            if (!state.State.Origins.TryGetValue(origin, out var entry))
            {
                entry = new ReplicationTreeOriginFrontier();
                state.State.Origins[origin] = entry;
            }

            if (entry.AwaitingPin)
            {
                entry.LowWatermark = lowWatermark;
                entry.AwaitingPin = false;
            }
            else if (lowWatermark > entry.LowWatermark)
            {
                entry.LowWatermark = lowWatermark;
            }

            if (entry.Capped)
            {
                // The tree now reflects the origin's writes below the export's
                // watermark; the cap holds there until the origin re-covers it.
                await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(origin)
                    .SetTreeCapAsync(TreeId, entry.LowWatermark, cancellationToken)
                    .ConfigureAwait(true);
            }
        }

        await WriteStateAsync().ConfigureAwait(true);
        PublishModes();
        return true;
    }

    /// <inheritdoc />
    public async Task<ReplicationTreeFrontierSnapshot> GetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureSettledAsync(cancellationToken).ConfigureAwait(true);

        var watermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var acked = new Dictionary<string, ReplicationAckedPositions>(StringComparer.Ordinal);
        if (state.State.Epoch != Guid.Empty)
        {
            foreach (var (origin, entry) in state.State.Origins)
            {
                if (!entry.AwaitingPin && entry.LowWatermark > HybridLogicalClock.Zero)
                {
                    watermarks[origin] = entry.LowWatermark;
                }

                if (!entry.AwaitingPin && entry.AckedPositions is { } positions)
                {
                    acked[origin] = positions;
                }
            }
        }

        return new ReplicationTreeFrontierSnapshot
        {
            Epoch = state.State.Epoch,
            RegistryLineage = state.State.ObservedRegistryLineage,
            LowWatermarks = watermarks,
            AckedPositions = acked,
            KnownOrigins = state.State.Epoch == Guid.Empty ? Array.Empty<string>() : [.. state.State.Origins.Keys],
        };
    }

    /// <inheritdoc />
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken token)
    {
        // Withdraw this activation's contribution; the next one republishes it.
        foreach (var (origin, mode) in _published)
        {
            LatticeReplicationMetrics.CausalFrontierOrigins.Add(
                -1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, origin),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagMode, mode),
                LatticeTenantLabel.ForTree(TreeId));
        }

        _published.Clear();
        if (!_watermarksDirty)
        {
            return;
        }

        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Persisting the applied low watermarks of tree {Tree} at deactivation failed; they are re-shipped", TreeId);
        }
    }

    /// <summary>
    /// Settles the epoch against the registry lineage once per activation and
    /// after an announced replacement. A lineage that differs from the one last
    /// observed - a replacement this grain was not told about - re-mints the
    /// epoch; none at all puts the tree in degraded mode.
    /// </summary>
    private async Task EnsureSettledAsync(CancellationToken cancellationToken)
    {
        if (_settledThisActivation && !state.State.Unsettled)
        {
            return;
        }

        var lineage = await lineageSource.GetLineageAsync(TreeId, cancellationToken).ConfigureAwait(true);
        var current = state.State;
        if (lineage is null)
        {
            if (current.Epoch != Guid.Empty)
            {
                await RestampAsync(Guid.Empty, cancellationToken, preserveBootstrapFloor: false).ConfigureAwait(true);
            }
        }
        else if (current.Unsettled)
        {
            // The announced replacement already minted the epoch; this only
            // records the lineage it produced (or kept, when it was abandoned).
            if (current.Epoch == Guid.Empty)
            {
                await RestampAsync(Guid.NewGuid(), cancellationToken, preserveBootstrapFloor: false).ConfigureAwait(true);
            }
        }
        else if (current.Epoch == Guid.Empty || lineage != current.ObservedRegistryLineage)
        {
            await RestampAsync(Guid.NewGuid(), cancellationToken, preserveBootstrapFloor: false).ConfigureAwait(true);
        }

        if (current.Unsettled || current.ObservedRegistryLineage != lineage)
        {
            current.Unsettled = false;
            current.ObservedRegistryLineage = lineage;
            await WriteStateAsync().ConfigureAwait(true);
        }

        _settledThisActivation = true;
    }

    /// <summary>
    /// Re-mints the epoch as <paramref name="epoch"/>: every origin's watermark
    /// drops to zero and awaits a re-seed, each origin's aggregate is capped at
    /// zero for this tree, and the tree's applied identities are forgotten. The
    /// caps land before the new epoch is durable, so a crash in between leaves
    /// the old epoch to be re-stamped again rather than an uncapped aggregate.
    /// </summary>
    private async Task RestampAsync(Guid epoch, CancellationToken cancellationToken, bool preserveBootstrapFloor)
    {
        foreach (var origin in state.State.Origins.Keys)
        {
            await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(origin)
                .SetTreeCapAsync(TreeId, HybridLogicalClock.Zero, cancellationToken)
                .ConfigureAwait(true);
        }

        await grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(TreeId)
            .ResetAppliedIdentitiesAsync(cancellationToken, preserveBootstrapFloor)
            .ConfigureAwait(true);

        foreach (var entry in state.State.Origins.Values)
        {
            entry.LowWatermark = HybridLogicalClock.Zero;
            entry.AwaitingPin = true;
            entry.Capped = true;
            entry.AckedPositions = null;
        }

        _forwarded.Clear();
        _capConfirmed.Clear();
        state.State.Epoch = epoch;
        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            // The in-memory state no longer matches the durable one; reload it
            // rather than serve either. The caller sees the failure and does not
            // replace the contents.
            this.DeactivateOnIdle();
            throw;
        }
    }

    /// <summary>
    /// Drops the export-held writes of <paramref name="originClusterId"/> below
    /// its newly accepted watermark. Best effort: a held write left listed only
    /// delays a dependent, and the next accepted watermark retries.
    /// </summary>
    private async Task DropExportHeldBelowAsync(
        IReplicationOriginFrontierGrain origin,
        string originClusterId,
        HybridLogicalClock below,
        CancellationToken cancellationToken)
    {
        try
        {
            var remaining = await origin.DropExportHeldBelowAsync(TreeId, below, cancellationToken).ConfigureAwait(true);
            if (remaining == 0)
            {
                state.State.ExportHeldOrigins.Remove(originClusterId);
                _watermarksDirty = true;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Dropping the export-held writes of origin {Origin} for tree {Tree} failed; retried on its next watermark", originClusterId, TreeId);
        }
    }

    /// <summary>The mode <paramref name="entry"/> contributes to the frontier-origins gauge.</summary>
    internal static string ModeOf(Guid epoch, ReplicationTreeOriginFrontier entry) =>
        epoch == Guid.Empty ? "degraded"
        : entry.AwaitingPin ? "awaiting_reseed"
        : entry.LowWatermark == HybridLogicalClock.Zero ? "pending"
        : "exact";

    private void PublishModes()
    {
        foreach (var (origin, entry) in state.State.Origins)
        {
            var mode = ModeOf(state.State.Epoch, entry);
            if (_published.TryGetValue(origin, out var previous))
            {
                if (string.Equals(previous, mode, StringComparison.Ordinal))
                {
                    continue;
                }

                LatticeReplicationMetrics.CausalFrontierOrigins.Add(
                -1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, origin),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagMode, previous),
                LatticeTenantLabel.ForTree(TreeId));
            }

            LatticeReplicationMetrics.CausalFrontierOrigins.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, origin),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagMode, mode),
                LatticeTenantLabel.ForTree(TreeId));
            _published[origin] = mode;
        }
    }

    private async Task MaybePersistWatermarksAsync()
    {
        if (!_watermarksDirty
            || Environment.TickCount64 - _persistedAt < (long)WatermarkPersistInterval.TotalMilliseconds)
        {
            return;
        }

        try
        {
            await WriteStateAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Persisting the applied low watermarks of tree {Tree} failed; retried on the next push", TreeId);
        }
    }

    private async Task WriteStateAsync()
    {
        await state.WriteStateAsync().ConfigureAwait(true);
        _watermarksDirty = false;
        _persistedAt = Environment.TickCount64;
    }
}
