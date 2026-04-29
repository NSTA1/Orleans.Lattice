using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

internal sealed class LatticeOptionsValidator : IValidateOptions<LatticeOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeOptions options)
    {
        if (options.KeysPageSize <= 0)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.KeysPageSize)} must be greater than 0.");
        if (options.MaxLeafReplayEntries < 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxLeafReplayEntries)} must be greater than or equal to 1.");
        return ValidateOptionsResult.Success;
    }
}
