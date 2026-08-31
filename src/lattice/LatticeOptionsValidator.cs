using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

internal sealed class LatticeOptionsValidator : IValidateOptions<LatticeOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeOptions options)
    {
        if (options.KeysPageSize <= 0)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.KeysPageSize)} must be greater than 0.");
        if (options.QueueCapacity is { } queueCapacity && queueCapacity < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.QueueCapacity)} must be greater than or equal to 1 when set "
                + "(null leaves the cluster-internal queue unbounded; a positive value caps it with FIFO eviction).");
        }
        if (options.MaxKeyLength is { } maxKeyLength && maxKeyLength < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxKeyLength)} must be greater than or equal to 1 when set "
                + "(null leaves key length unbounded; a positive value caps the number of characters in a key).");
        }
        if (options.MaxValueSizeBytes is { } maxValueSizeBytes && maxValueSizeBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxValueSizeBytes)} must be greater than or equal to 1 when set "
                + "(null leaves value size unbounded; a positive value caps the byte length of a value or CRDT delta).");
        }
        if (options.MaxCacheValueBytes is { } maxCacheValueBytes && maxCacheValueBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxCacheValueBytes)} must be greater than or equal to 1 when set "
                + "(null leaves the read-through cache mirror unbounded; a positive value caps the resident value-payload bytes per cache activation with LRU payload eviction).");
        }
        if (options.LeafCachePreWarmCount < 0
            || options.LeafCachePreWarmCount > LatticeOptions.MaxLeafCachePreWarmCount)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafCachePreWarmCount)} must be between 0 and "
                + $"{LatticeOptions.MaxLeafCachePreWarmCount} inclusive "
                + "(0 disables leaf-access tracking and post-restart leaf-cache pre-warm; the upper bound matches the "
                + "number of leaves the shard root persists, so a larger request could not be satisfied).");
        }
        if (options.LeafAccessModelFlushIntervalMs < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafAccessModelFlushIntervalMs)} must be greater than or equal to 0 "
                + "(0 persists the leaf-access model only on clean deactivation; a positive value is the coalescing "
                + "window in milliseconds for the shard-root flush timer).");
        }
        if (options.LeafHydrationResidentBytes < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafHydrationResidentBytes)} must be greater than or equal to 0 "
                + "(0 leaves a partially hydrated leaf's resident snapshot footprint unbounded so nothing is ever "
                + "evicted; a positive value caps it and evicts the least recently used clean hydrated ranges).");
        }
        if (options.MaxClusterConcurrentAutoSplits is { } maxClusterConcurrentAutoSplits && maxClusterConcurrentAutoSplits < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxClusterConcurrentAutoSplits)} must be greater than or equal to 1 when set "
                + "(null disables the cluster-wide split gate so each tree enforces only its own MaxConcurrentAutoSplits; "
                + "a positive value caps the aggregate number of concurrently in-flight autonomic splits across all trees).");
        }
        if (double.IsNaN(options.HotShardMinSkewRatio) || options.HotShardMinSkewRatio < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.HotShardMinSkewRatio)} must be a non-negative number "
                + "(it is the ratio of the hottest shard's rate to the tree's median shard rate at which an "
                + "autonomic split is admitted; a value at or below 1.0 disables the skew gate).");
        }
        if (double.IsNaN(options.HotShardConsolidationSkewRatio) || options.HotShardConsolidationSkewRatio < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.HotShardConsolidationSkewRatio)} must be a non-negative number "
                + "(it is the load-skew ratio at or below which a tree counts as uniformly loaded and therefore "
                + "eligible for shard consolidation).");
        }
        if (options.HotShardMinSkewRatio > 1d
            && options.HotShardConsolidationSkewRatio >= options.HotShardMinSkewRatio)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.HotShardConsolidationSkewRatio)} must be strictly less than "
                + $"{nameof(LatticeOptions.HotShardMinSkewRatio)} "
                + "(the interval between them is the hysteresis dead band separating the split trigger from the "
                + "consolidation trigger; overlapping trigger regions let the two control loops oscillate, "
                + "splitting a tree and immediately consolidating it again).");
        }
        if (options.HotShardMinShardEntries < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.HotShardMinShardEntries)} must be greater than or equal to 0 "
                + "(0 disables the occupancy floor and its per-candidate probe; a positive value is the minimum "
                + "number of live entries a shard must hold before an autonomic split can relieve anything).");
        }
        if (options.ConsolidationDrainBatchSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ConsolidationDrainBatchSize)} must be greater than or equal to 1 "
                + "(it is the number of donor entries the online shard-consolidation coordinator accumulates per "
                + "MergeManyAsync flush to the survivor; there is no meaningful 'disabled' value, because a fold that "
                + "flushes no entries could never drain the donor).");
        }
        if (options.ConsolidationDrainLeavesPerPass < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ConsolidationDrainLeavesPerPass)} must be greater than or equal to 1 "
                + "(it is the number of donor leaves the online shard-consolidation coordinator visits per background "
                + "pass; 0 would visit no leaf per pass, so a fold would never make progress and would sit in its "
                + "drain phase indefinitely rather than being disabled).");
        }
        if (options.MaxConcurrentShardConsolidations < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxConcurrentShardConsolidations)} must be greater than or equal to 0 "
                + "(0 admits no online shard consolidation at all, which is the supported way to switch automated "
                + "shard healing off; a positive value caps how many folds a driver may run concurrently against "
                + "one tree).");
        }
        if (options.ShardHealingInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ShardHealingInterval)} must be greater than zero "
                + "(it is the cadence at which the healing orchestrator observes a tree's shape and admits at most "
                + "one consolidation; a non-positive interval could never schedule an observation, so it is rejected "
                + $"rather than treated as disabled - set {nameof(LatticeOptions.ShardHealingEnabled)} to false to "
                + "switch automatic over-split healing off).");
        }
        if (options.ShardHealingCooldown < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ShardHealingCooldown)} must be greater than or equal to zero "
                + "(zero disables the post-split stand-off window, leaving only the skew dead band between the split "
                + "and consolidation triggers; a positive value is how long healing waits after observing a split "
                + "before consolidating the same tree).");
        }
        if (double.IsNaN(options.ShardHealingBackpressureOpsPerSecond)
            || options.ShardHealingBackpressureOpsPerSecond < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ShardHealingBackpressureOpsPerSecond)} must be a non-negative number "
                + "(0 disables backpressure so healing proceeds regardless of load; a positive value is the tree's "
                + "median shard rate in operations per second at or above which healing yields to foreground "
                + "traffic).");
        }
        if (options.MaxLiveKeys is { } maxLiveKeys && maxLiveKeys < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxLiveKeys)} must be greater than or equal to 1 when set "
                + "(null leaves the live-key count unbounded; a positive value caps the number of live keys per tree, rejecting further writes with LatticeQuotaExceededException).");
        }
        if (options.MaxEstimatedBytes is { } maxEstimatedBytes && maxEstimatedBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaxEstimatedBytes)} must be greater than or equal to 1 when set "
                + "(null leaves estimated storage unbounded; a positive value caps the estimated retained bytes per tree, rejecting further writes with LatticeQuotaExceededException).");
        }
        if (options.AdmissionAdvisoryLiveKeys is { } advisoryLiveKeys && advisoryLiveKeys < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.AdmissionAdvisoryLiveKeys)} must be greater than or equal to 1 when set "
                + "(null disables the live-key advisory dry-run signal; a positive value drives the over-advisory and would-reject metrics without rejecting any write).");
        }
        if (options.AdmissionAdvisoryBytes is { } advisoryBytes && advisoryBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.AdmissionAdvisoryBytes)} must be greater than or equal to 1 when set "
                + "(null disables the byte advisory dry-run signal; a positive value drives the over-advisory and would-reject metrics without rejecting any write).");
        }
        if (options.MaxLeafReplayEntries < 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxLeafReplayEntries)} must be greater than or equal to 1.");
        if (options.MaterialiserCheckpointEntries < 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaterialiserCheckpointEntries)} must be greater than or equal to 1.");
        if (options.MaterialiserCheckpointInterval < TimeSpan.Zero
            && options.MaterialiserCheckpointInterval != Timeout.InfiniteTimeSpan)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaterialiserCheckpointInterval)} must be non-negative or {nameof(Timeout.InfiniteTimeSpan)}.");
        }
        if (options.LeafProjectionRetention <= TimeSpan.Zero
            && options.LeafProjectionRetention != Timeout.InfiniteTimeSpan)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafProjectionRetention)} must be positive or {nameof(Timeout.InfiniteTimeSpan)}.");
        }
        if (!Enum.IsDefined(options.ProjectionRebuildPolicy))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ProjectionRebuildPolicy)} must be a defined {nameof(ProjectionRebuildPolicy)} value.");
        }
        if (options.LeafSnapshotMargin < 0.0 || options.LeafSnapshotMargin > 1.0
            || double.IsNaN(options.LeafSnapshotMargin))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafSnapshotMargin)} must be in the inclusive range [0.0, 1.0].");
        }
        if (options.LeafSnapshotReClassifyEveryNCheckpoints < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints)} must be greater than or equal to 0 (0 disables the periodic re-classification).");
        }
if (options.WalMaxPendingBatches < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalMaxPendingBatches)} must be greater than or equal to 1. "
        + "The in-memory backlog cap must permit at least one in-flight flush.");
}
if (options.MaxSnapshotReplayEntries < 1)
    return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxSnapshotReplayEntries)} must be greater than or equal to 1.");
if (options.WalPartitions < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalPartitions)} must be greater than or equal to 1. "
        + "Set to 1 (the default) to retain the single-partition WAL shape; raise to fan out WAL throughput across independent grains.");
}
if (options.SnapshotLeafIdleTtl <= TimeSpan.Zero
    && options.SnapshotLeafIdleTtl != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.SnapshotLeafIdleTtl)} must be positive or {nameof(Timeout.InfiniteTimeSpan)}.");
}
if (options.SnapshotBaselineTtl <= TimeSpan.Zero
    && options.SnapshotBaselineTtl != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.SnapshotBaselineTtl)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the leak-guard retention window for durably-persisted frozen snapshot baselines; infinite disables the backstop reminder).");
}
if (options.WalFlushTimeout <= TimeSpan.Zero
    && options.WalFlushTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalFlushTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the in-flight flush deadline that prevents a hung provider call from wedging the in-flight chain).");
}
if (options.ShardForwardTimeout <= TimeSpan.Zero
    && options.ShardForwardTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.ShardForwardTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the outbound shard-forward deadline that prevents a parked forward during reshard swap from wedging the write pipeline).");
}
if (options.ActivationReadyTimeout <= TimeSpan.Zero
    && options.ActivationReadyTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.ActivationReadyTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the shard-root activation-readiness seed deadline that prevents a parked first-activation RPC from wedging the write pipeline).");
}
if (options.EmptyTreeProbeBudget <= TimeSpan.Zero
    && options.EmptyTreeProbeBudget != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.EmptyTreeProbeBudget)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the reshard-initiation emptiness-probe ceiling that stops a strongly-consistent count from timing the reshard out under concurrent split churn).");
}
if (options.DigestPublishTimeout <= TimeSpan.Zero
    && options.DigestPublishTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.DigestPublishTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the internal-node digest publish deadline that prevents a parked upward publish from pinning the split gate).");
}
if (options.WalAppendDispatchTimeout <= TimeSpan.Zero
    && options.WalAppendDispatchTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalAppendDispatchTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the writer-side cross-grain dispatch deadline that prevents a wedged WAL shard from holding every caller's dispatch parked until the Orleans response timeout).");
}
if (options.WalFlushPreflightTimeout <= TimeSpan.Zero
    && options.WalFlushPreflightTimeout != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalFlushPreflightTimeout)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the per-shard flush preflight deadline that prevents a parked scheduler yield from pinning an in-flight slot with no deadline armed).");
}
if (options.WalDrainBudget <= TimeSpan.Zero
    && options.WalDrainBudget != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalDrainBudget)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the per-shard deactivation drain budget that prevents a wedged provider call from holding host shutdown indefinitely).");
}
if (options.WalSaturationSampleInterval <= TimeSpan.Zero
    && options.WalSaturationSampleInterval != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationSampleInterval)} must be positive or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the saturation sampler cadence; infinite disables the sampler entirely and pins every tree's signal to Healthy).");
}
if (options.WalSaturationThrottledRatio < 0.0 || options.WalSaturationThrottledRatio > 1.0
    || double.IsNaN(options.WalSaturationThrottledRatio))
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationThrottledRatio)} must be in the inclusive range [0.0, 1.0].");
}
if (options.WalSaturationDispatchTimeoutThreshold < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationDispatchTimeoutThreshold)} must be greater than or equal to 1.");
}
if (options.WalSaturationProviderFailureRateThreshold < 0)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationProviderFailureRateThreshold)} must be greater than or equal to 0 "
        + "(zero disables the provider-failure-rate trigger entirely; a positive value sets the per-sample-window failure count that raises a tree to Saturated regardless of admission depth).");
}
if (options.WalSaturationRecoveryWindow < TimeSpan.Zero
    && options.WalSaturationRecoveryWindow != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationRecoveryWindow)} must be non-negative or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the recovery window holds a tree at Throttled after the most-recent Saturated observation; zero disables the window entirely, infinite holds Throttled forever after the first Saturated observation).");
}
if (options.WalSaturationFlushLatencyThreshold is { } flushLatencyThreshold
    && flushLatencyThreshold <= TimeSpan.Zero)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationFlushLatencyThreshold)} must be positive when set "
        + "(null disables the flush-latency classifier input entirely; a positive value sets the per-flush "
        + "wal.append.provider.duration above which the per-(tree, shard) trip counter is incremented).");
}
if (options.WalSaturationFlushLatencySampleWindows < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationFlushLatencySampleWindows)} must be greater than or equal to 1 "
        + "(the number of consecutive sampler ticks the per-tree flush-latency trip delta must be non-zero "
        + "before the classifier escalates to Saturated via the flush-latency branch).");
}
if (options.WalSaturationMaterialiserLagThreshold is { } materialiserLagThreshold
    && materialiserLagThreshold <= TimeSpan.Zero)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationMaterialiserLagThreshold)} must be positive when set "
        + "(null disables the materialiser drain-lag classifier input entirely; a positive value sets the "
        + "leaf-materialiser drain lag - WAL head wall-clock minus the slowest durable checkpoint frontier - above "
        + "which the per-tree standing lag level counts as over-threshold for the consecutive-window classifier).");
}
if (options.WalSaturationMaterialiserLagSampleWindows < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalSaturationMaterialiserLagSampleWindows)} must be greater than or equal to 1 "
        + "(the number of consecutive saturation-sampler windows the tree's drain-lag level must exceed the threshold "
        + "before the classifier holds the tree at Throttled via the drain-lag branch).");
}
if (options.WalMaterialiserMaxConcurrentReplays < 0)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalMaterialiserMaxConcurrentReplays)} must be greater than or equal to 0 "
        + "(zero resolves the per-silo concurrent-leaf-replay ceiling to Environment.ProcessorCount; a positive "
        + "value pins it explicitly).");
}
if (options.WalReplayMaxRecordsPerTurn < 0)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalReplayMaxRecordsPerTurn)} must be greater than or equal to 0 "
        + "(zero disables the cooperative activation-replay yield; a positive value bounds the number of WAL "
        + "records applied per scheduler turn before the replay yields).");
}
if (options.WalAdmissionSaturationWaitBudget < TimeSpan.Zero
    && options.WalAdmissionSaturationWaitBudget != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalAdmissionSaturationWaitBudget)} must be non-negative or {nameof(Timeout.InfiniteTimeSpan)} "
        + "(the admission-gate budget the WAL writer waits on WaitForHealthyAsync under Saturated before refusing the dispatch with LatticeSaturatedException; zero disables the gate, infinite waits forever for recovery).");
}
if (options.WalThrottledAdmissionPace < TimeSpan.Zero)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalThrottledAdmissionPace)} must be non-negative "
        + "(the per-append local-path pacing delay the WAL writer applies while the tree is Throttled; zero disables local pacing).");
}
return ValidateOptionsResult.Success;
    }
}
