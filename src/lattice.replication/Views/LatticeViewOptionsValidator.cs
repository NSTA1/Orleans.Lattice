using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Validates <see cref="LatticeViewOptions"/> at first resolve: the per-pass
/// batch size must be positive and the coalesce window must be greater than zero.
/// Mirrors how the replication options are validated.
/// </summary>
internal sealed class LatticeViewOptionsValidator : IValidateOptions<LatticeViewOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeViewOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        if (options.BatchSize <= 0)
        {
            failures.Add($"{nameof(LatticeViewOptions.BatchSize)} must be positive (was {options.BatchSize}).");
        }

        if (options.CoalesceWindow <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeViewOptions.CoalesceWindow)} must be greater than zero (was {options.CoalesceWindow}).");
        }

        if (options.AggregationFanout < 1)
        {
            failures.Add($"{nameof(LatticeViewOptions.AggregationFanout)} must be at least 1 (was {options.AggregationFanout}).");
        }

        if (options.AggregationMaxGroupEntries < 0)
        {
            failures.Add($"{nameof(LatticeViewOptions.AggregationMaxGroupEntries)} must not be negative (was {options.AggregationMaxGroupEntries}).");
        }

        if (options.MaxStagedTransactions < 1)
        {
            failures.Add($"{nameof(LatticeViewOptions.MaxStagedTransactions)} must be at least 1 (was {options.MaxStagedTransactions}).");
        }

        if (options.MaxStagedBytes < 1)
        {
            failures.Add($"{nameof(LatticeViewOptions.MaxStagedBytes)} must be at least 1 (was {options.MaxStagedBytes}).");
        }

        if (options.ReadHandleCacheTtl <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeViewOptions.ReadHandleCacheTtl)} must be greater than zero (was {options.ReadHandleCacheTtl}).");
        }

        if (options.OldGenerationReclaimGrace <= options.ReadHandleCacheTtl)
        {
            failures.Add(
                $"{nameof(LatticeViewOptions.OldGenerationReclaimGrace)} ({options.OldGenerationReclaimGrace}) must exceed " +
                $"{nameof(LatticeViewOptions.ReadHandleCacheTtl)} ({options.ReadHandleCacheTtl}) so a stale reader's cached generation is never reclaimed under it.");
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
