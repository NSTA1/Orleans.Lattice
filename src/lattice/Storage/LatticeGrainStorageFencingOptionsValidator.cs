using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

internal sealed class LatticeGrainStorageFencingOptionsValidator
    : IValidateOptions<LatticeGrainStorageFencingOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeGrainStorageFencingOptions options)
    {
        if (!Enum.IsDefined(options.Mode))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeGrainStorageFencingOptions.Mode)} must be one of "
                + $"{string.Join(", ", Enum.GetNames<LatticeGrainStorageFencingMode>())}.");
        }

        if (options.ProbeTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeGrainStorageFencingOptions.ProbeTimeout)} must be positive.");
        }

        return ValidateOptionsResult.Success;
    }
}
