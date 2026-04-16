using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

internal sealed class LatticeOptionsValidator : IValidateOptions<LatticeOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeOptions options)
    {
        if (options.ShardCount <= 0)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.ShardCount)} must be greater than 0.");
        if (options.MaxLeafKeys <= 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxLeafKeys)} must be greater than 1.");
        if (options.MaxInternalChildren <= 2)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxInternalChildren)} must be greater than 2.");
        if (options.KeysPageSize <= 0)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.KeysPageSize)} must be greater than 0.");
        return ValidateOptionsResult.Success;
    }
}
