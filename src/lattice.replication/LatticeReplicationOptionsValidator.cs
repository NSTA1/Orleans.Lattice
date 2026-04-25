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
        if (string.IsNullOrWhiteSpace(options.ClusterId))
        {
            var scope = string.IsNullOrEmpty(name)
                ? "default options instance"
                : $"options instance '{name}'";
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ClusterId)} "
                + $"must be set to a non-empty, globally-unique identifier for the local Orleans cluster ({scope}). "
                + "Replication stamps this value on every captured mutation so receivers can attribute "
                + "origin and break replication cycles; an empty value would produce unattributable "
                + "change-feed entries and is rejected.");
        }

        return ValidateOptionsResult.Success;
    }
}

