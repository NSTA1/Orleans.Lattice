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

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
