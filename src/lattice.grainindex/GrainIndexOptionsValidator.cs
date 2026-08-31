using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Validates the per-index <see cref="GrainIndexOptions"/> resolved by index
/// name. Every failure names the offending index, because a silo may declare
/// many and an unqualified message would not say which one to fix.
/// </summary>
internal sealed class GrainIndexOptionsValidator : IValidateOptions<GrainIndexOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, GrainIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // The unnamed instance is the template the named ones are built from and
        // never backs an index, so it carries no tree name to validate.
        if (string.IsNullOrEmpty(name))
        {
            return ValidateOptionsResult.Skip;
        }

        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.TreeName))
        {
            failures.Add(
                $"Grain index '{name}' has no backing tree name. "
                + $"{nameof(GrainIndexOptions.TreeName)} must be set; leave it at its default to get "
                + $"'{GrainIndexTreeNames.ForIndex(name)}'.");
        }
        else if (!GrainIndexTreeNames.IsIndexOwned(options.TreeName))
        {
            failures.Add(
                $"Grain index '{name}' has backing tree name '{options.TreeName}', which is outside the "
                + $"reserved '{GrainIndexTreeNames.ReservedPrefix}' namespace. Index-owned trees stay "
                + "inside it so they are identifiable as cluster-local by intent.");
        }

        if (options.BackfillBatchSize < 1)
        {
            failures.Add(
                $"Grain index '{name}' has {nameof(GrainIndexOptions.BackfillBatchSize)} "
                + $"{options.BackfillBatchSize}; it must be at least 1 so a backfill pass makes progress.");
        }

        if (options.BackfillInterval <= TimeSpan.Zero)
        {
            failures.Add(
                $"Grain index '{name}' has {nameof(GrainIndexOptions.BackfillInterval)} "
                + $"{options.BackfillInterval}; it must be greater than zero so the backfill is paced "
                + "against foreground traffic.");
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
