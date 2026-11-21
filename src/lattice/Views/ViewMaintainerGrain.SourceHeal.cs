using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// The sliced source-identity heal (#5546). When the view's source is swapped
/// underneath its alias, the maintainer rebuilds the next generation from the new
/// physical source before rebinding. Rather than run that whole rebuild in one
/// grain turn, which made the activation unresponsive for the duration of the
/// reprojection, the heal projects at most <see cref="HealSliceKeyBudget"/> keys
/// per drain. The first drain also begins the build (captures floors and clears
/// the shadow) and scans the source key set; the drain that projects the last
/// key swaps. A source within the budget therefore heals in one drain, as
/// before. Progress lives in
/// memory; a deactivation mid-heal restarts it, and the restart re-clears the
/// partial shadow generation.
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>
    /// Maximum number of source keys one drain re-projects while a sliced
    /// source-identity heal is in progress. Settable so a unit test can force a
    /// multi-slice heal over a small source.
    /// </summary>
    internal int HealSliceKeyBudget { get; set; } = 1024;

    // In-progress sliced heal, or null when none is running. Invalidated by an
    // epoch bump (explicit rebuild / reconcile / decommission or its cancel), a
    // generation change, or a further source swap.
    private ShadowBuildProgress? _shadowHeal;

    /// <summary>
    /// Advances the source-identity heal by one bounded step. Returns
    /// <see langword="true"/> once the rebuilt generation has been swapped in and
    /// the new physical identity published; <see langword="false"/> while the
    /// heal still has steps to run on later drains.
    /// </summary>
    /// <remarks>
    /// Only the derive-locally shadow build is sliced. A ShipView in-place rebuild,
    /// or a suppressed consumer, has no shadow to stage, so it falls back to the
    /// single-call <see cref="RebuildAsync"/> exactly as before.
    /// </remarks>
    private async Task<bool> AdvanceSourceHealAsync(
        ViewRegistration registration,
        string physical,
        CancellationToken cancellationToken)
    {
        var options = Options;
        if (options.ReplicationMode == LatticeViewReplicationMode.ShipView)
        {
            _shadowHeal = null;
            await RebuildAsync(cancellationToken);
            return true;
        }

        var maintenanceRole = ViewReplicationTopology.Resolve(
            ViewName,
            registration.SourceTreeId,
            options,
            replicationContext,
            GenerationTreeId(state.State.ActiveGeneration + 1));
        if (maintenanceRole == ViewReplicationTopology.MaintenanceRole.Suppress
            || (maintenanceRole == ViewReplicationTopology.MaintenanceRole.InferFromSource
                && !await IsSourceLocallyReadableAsync(registration, cancellationToken)))
        {
            _shadowHeal = null;
            await RebuildAsync(cancellationToken);
            return true;
        }

        await GuardJointMaintenanceAdmissionAsync(cancellationToken);

        // A heal step is maintenance work an imminent decommission cancels (#5729).
        using var maintenanceLink = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, MaintenanceToken);
        cancellationToken = maintenanceLink.Token;

        using var viewWriteScope = ViewWriteContext.BeginScope();
        await ReleaseUncommittedJointOwnershipAsync();
        if (HasJointOwnership)
        {
            _shadowHeal = null;
            throw new InvalidOperationException($"View '{ViewName}' cannot rebuild before its submitted joint repair checkpoint retires.");
        }

        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            options,
            state.State.ActiveGeneration);

        try
        {
            var heal = _shadowHeal;
            if (heal is not null && !IsShadowHealCurrent(heal, registration, physical))
            {
                heal = null;
                _shadowHeal = null;
            }

            if (heal is null)
            {
                heal = await BeginShadowBuildAsync(registration, cancellationToken);
                _shadowHeal = heal;
            }

            if (heal.Keys is null)
            {
                await ScanShadowKeysAsync(heal, cancellationToken);
            }

            if (!await ProjectShadowSliceAsync(heal, HealSliceKeyBudget, cancellationToken))
            {
                return false;
            }

            _shadowHeal = null;
            await ReportMaintenanceProgressAsync(Operations.LatticeMaintenanceProgress.Swapping);
            await SwapToShadowAsync(registration, heal.ToResult(), cancellationToken);
            return true;
        }
        catch
        {
            // A failed step restarts the heal from the beginning on the next drain.
            _shadowHeal = null;
            throw;
        }
    }

    /// <summary>
    /// Whether an in-progress heal still targets the current source, generation
    /// and maintenance epoch; a stale heal is abandoned and restarted.
    /// </summary>
    private bool IsShadowHealCurrent(ShadowBuildProgress heal, ViewRegistration registration, string physical) =>
        heal.Epoch == _healEpoch
        && heal.BaseGeneration == state.State.ActiveGeneration
        && string.Equals(heal.WalTreeId, physical, StringComparison.Ordinal)
        && string.Equals(heal.Registration.SourceTreeId, registration.SourceTreeId, StringComparison.Ordinal);

    /// <summary>
    /// The resumable state of one shadow build: what <c>BeginShadowBuildAsync</c>
    /// captured, the scanned key set, and how far the projection has advanced.
    /// </summary>
    private sealed class ShadowBuildProgress(
        ViewRegistration registration,
        string walTreeId,
        Dictionary<int, long> capturedOffsets,
        long baseGeneration,
        long epoch,
        ILattice shadowTree,
        AggregationApplier? aggregationApplier,
        HistoryRetentionPolicy historyPolicy,
        long historyNowTicks)
    {
        public ViewRegistration Registration { get; } = registration;

        public string WalTreeId { get; } = walTreeId;

        public Dictionary<int, long> CapturedOffsets { get; } = capturedOffsets;

        public long BaseGeneration { get; } = baseGeneration;

        public long Epoch { get; } = epoch;

        public ILattice ShadowTree { get; } = shadowTree;

        public AggregationApplier? AggregationApplier { get; } = aggregationApplier;

        public HistoryRetentionPolicy HistoryPolicy { get; } = historyPolicy;

        public long HistoryNowTicks { get; } = historyNowTicks;

        /// <summary>The scanned source keys; <see langword="null"/> until the scan step has run.</summary>
        public string[]? Keys { get; set; }

        /// <summary>Index of the next key to project.</summary>
        public int NextIndex { get; set; }

        /// <summary>The highest source version projected so far.</summary>
        public HybridLogicalClock Highest { get; set; } = HybridLogicalClock.Zero;

        public ShadowBuildResult ToResult() => new(WalTreeId, CapturedOffsets, Highest);
    }
}
