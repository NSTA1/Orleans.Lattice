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

        return ValidateOptionsResult.Success;
    }
}

