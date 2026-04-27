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

        if (options.WalRetention is { } retention && retention <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.WalRetention)} "
                + $"must be strictly greater than {nameof(TimeSpan)}.{nameof(TimeSpan.Zero)} when set ({scope}). "
                + "A zero or negative retention would render every entry trim-eligible the moment it lands; "
                + "leave the property unset to disable the wall-clock ceiling entirely.");
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

