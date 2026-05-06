using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IValidateOptions{TOptions}"/> implementation that fails fast
/// when <see cref="LatticeReplicationOptions"/> is misconfigured. Runs the
/// first time the options are resolved (lazy), so a host that registers
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// without setting <see cref="LatticeReplicationOptions.ClusterId"/> sees a
/// clear validation error rather than producing
/// <see cref="ReplogEntry"/> records with no attributable origin.
/// </summary>
internal sealed class LatticeReplicationOptionsValidator : IValidateOptions<LatticeReplicationOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeReplicationOptions options)
    {
        var scope = string.IsNullOrEmpty(name)
            ? "default options instance"
            : $"options instance '{name}'";

        if (string.IsNullOrWhiteSpace(options.ClusterId))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ClusterId)} "
                + $"must be set to a non-empty, globally-unique identifier for the local Orleans cluster ({scope}). "
                + "Replication stamps this value on every captured mutation so receivers can attribute "
                + "origin and break replication cycles; an empty value would produce unattributable "
                + "change-feed entries and is rejected.");
        }

        if (options.ReplogPartitions < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplogPartitions)} "
                + $"must be at least 1 ({scope}). The captured change-feed sink routes every "
                + $"{nameof(ReplogEntry)} to a single per-tree WAL grain keyed by "
                + "{treeId}/{partition}, where partition is hash(key) modulo this value; a value "
                + "of zero or less leaves no partitions to route to.");
        }

        if (options.WalMaxBatchEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxBatchEntries)} "
                + $"must be at least 1 ({scope}). The per-shard WAL grain refuses to flush a "
                + "zero-sized batch; a non-positive value would deadlock the commit-time observer.");
        }

        if (options.WalMaxBatchBytes < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxBatchBytes)} "
                + $"must be at least 1 ({scope}). The byte-budget cap on a single batch must "
                + "permit at least one entry; a non-positive value would block every flush.");
        }

        if (options.WalMaxPendingBatches < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalMaxPendingBatches)} "
                + $"must be at least 1 ({scope}). The in-memory backlog cap must permit at "
                + "least one pending batch alongside the in-flight flush.");
        }

        if (options.MaxApplyRetries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaxApplyRetries)} "
                + $"must be at least 1 ({scope}). The dead-letter routing threshold cannot be "
                + "zero; a value of one parks an entry on the first failure.");
        }

        if (options.DeadLetterQueueCapacity < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.DeadLetterQueueCapacity)} "
                + $"must be at least 1 ({scope}). A zero-capacity queue cannot accept the "
                + "very entry the apply pipeline is trying to park.");
        }

        if (options.CausalBufferMaxEntries < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.CausalBufferMaxEntries)} "
                + $"must be at least 1 ({scope}). The per-tree causal-apply buffer must "
                + "permit at least one blocked entry before overflowing to the dead-letter queue.");
        }

        if (options.CausalBufferMaxBytes < 65536)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.CausalBufferMaxBytes)} "
                + $"must be at least 65536 (64 KB) ({scope}). A smaller cap would force "
                + "every typical entry to overflow to the dead-letter queue immediately on park.");
        }

        if (options.ShadowForwardDedupeCacheSize < 64)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShadowForwardDedupeCacheSize)} "
                + $"must be at least 64 ({scope}). The shadow-forward dedupe cache must retain "
                + "enough recent identity tuples that a sustained burst of shadow-forwarded duplicates "
                + "cannot evict the cache faster than concurrent inbound deliveries race past the "
                + "per-origin high-water-mark check.");
        }

        if (options.AtomicBatchBufferMaxTransactions < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AtomicBatchBufferMaxTransactions)} "
                + $"must be at least 1 ({scope}). The per-tree atomic-batch staging buffer must permit "
                + "at least one in-flight transaction; a zero cap would force every admitted entry to "
                + "evict the same entry it just admitted.");
        }

        if (options.AtomicBatchBufferMaxBytes < 1L * 1024L * 1024L)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.AtomicBatchBufferMaxBytes)} "
                + $"must be at least 1048576 (1 MB) ({scope}). The byte cap on the per-tree atomic-batch "
                + "staging buffer must remain meaningfully larger than a typical single-entry payload "
                + "so a partially-buffered batch cannot trigger eviction on its first entry.");
        }

        if (options.WalRetention is { } retention && retention <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalRetention)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} when set ({scope}). "
                + "A zero or negative retention would render every entry trim-eligible the moment it lands; "
                + "leave the property unset to disable the wall-clock ceiling entirely.");
        }

        if (options.OperatorReseedMinInterval < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.OperatorReseedMinInterval)} "
                + $"must be greater than or equal to {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A negative interval has no meaningful interpretation; set the value to "
                + $"{nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} to disable operator re-seed rate limiting entirely.");
        }

        if (options.ShipBatchSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBatchSize)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain refuses to assemble a "
                + "zero-sized batch; a non-positive value would deadlock the outbound ship loop.");
        }

        if (options.ShipPartitionPageSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipPartitionPageSize)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain reads at least one entry "
                + "per partition per pump tick; a non-positive value would tight-loop the partition-resume drain.");
        }

        if (options.ShipCursorWriteInterval < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipCursorWriteInterval)} "
                + $"must be at least 1 ({scope}). Zero or negative values would suppress every durable cursor "
                + "write, so the WAL GC would never advance and a silo crash would replay the entire log.");
        }

        if (options.ShipMaxInFlight < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipMaxInFlight)} "
                + $"must be at least 1 ({scope}). The per-peer shipper grain must permit at least "
                + "one in-flight send for the outbound ship loop to make progress.");
        }

        if (options.ShipBackoffInitial <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffInitial)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative initial backoff would tight-loop the shipper through transient "
                + "transport failures; the doubling sequence needs a non-zero seed value.");
        }

        if (options.ShipPhaseTimerPeriod <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipPhaseTimerPeriod)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative phase-timer period would either tight-loop the shipper or never "
                + "fire the polling fallback when the doorbell signal is unavailable.");
        }

        if (options.ShipBackoffMax < options.ShipBackoffInitial)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffMax)} "
                + $"must be greater than or equal to {nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffInitial)} ({scope}). "
                + "The doubling sequence is capped at this value, so a cap below the seed leaves the "
                + "shipper unable to apply even the first backoff delay.");
        }

        if (options.ShipBackoffJitter is < 0.0 or > 1.0 or double.NaN)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ShipBackoffJitter)} "
                + $"must be in the closed interval [0.0, 1.0] ({scope}); got {options.ShipBackoffJitter}. "
                + "Jitter is a multiplicative factor applied as a +/- spread on each backoff delay; "
                + "0.0 disables jitter entirely, 1.0 randomises across the full +/-100 % range.");
        }

        if (options.MaintenanceGcInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaintenanceGcInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative GC cadence would tight-loop the maintenance grain through "
                + $"{nameof(ILatticeReplicationGc)}.{nameof(ILatticeReplicationGc.RunOnceAsync)} calls.");
        }

        if (options.MaintenanceFallOffCheckInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.MaintenanceFallOffCheckInterval)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative fall-off cadence would tight-loop the maintenance grain through "
                + $"{nameof(ILatticeFallOffLogDetector)}.{nameof(ILatticeFallOffLogDetector.CheckAndTriggerAsync)} calls.");
        }

        if (options.TxBufferOrphanTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.TxBufferOrphanTimeout)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} ({scope}). "
                + "A zero or negative orphan timeout would force the maintenance grain to evict every "
                + "partially-buffered atomic-batch transaction on the very first sweep tick after admission, "
                + "draining the buffer faster than batches can complete.");
        }

        if (options.ReplicatedTrees is { } trees)
        {
            foreach (var kvp in trees)
            {
                if (string.IsNullOrWhiteSpace(kvp.Key))
                {
                    return ValidateOptionsResult.Fail(
                        $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)} "
                        + $"must not contain null, empty, or whitespace tree-id keys ({scope}). "
                        + "Every replicated tree must be declared by its concrete tree id so the "
                        + "commit-time observer can resolve the per-tree replication mode.");
                }

                if (!Enum.IsDefined(kvp.Value))
                {
                    return ValidateOptionsResult.Fail(
                        $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)} "
                        + $"declares tree '{kvp.Key}' with an undefined "
                        + $"{nameof(ReplicationMode)} value '{(int)kvp.Value}' ({scope}). "
                        + $"Use one of {nameof(ReplicationMode.LwwRegister)}, "
                        + $"{nameof(ReplicationMode.OrSet)}, "
                        + $"{nameof(ReplicationMode.PnCounter)}, or "
                        + $"{nameof(ReplicationMode.VersionVector)}.");
                }
            }
        }

        return ValidateOptionsResult.Success;
    }
}

