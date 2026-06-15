using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

internal sealed class LatticeTagIndexReconciliationOptionsValidator
    : IValidateOptions<LatticeTagIndexReconciliationOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeTagIndexReconciliationOptions options)
    {
        if (options.Interval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeTagIndexReconciliationOptions.Interval)} must be positive "
                + $"(it is clamped up to {nameof(LatticeTagIndexReconciliationOptions.MinimumInterval)} "
                + "when the schedule reminder is registered).");
        }
        if (options.ChunkSize < 1)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeTagIndexReconciliationOptions.ChunkSize)} must be greater than or equal to 1 "
                + "(it bounds the number of covered trees processed per phase-timer tick).");
        }
        return ValidateOptionsResult.Success;
    }
}
