using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Validates <see cref="LatticeIdentityDirectoryOptions"/> at silo start: rejects
/// a non-positive default or maximum page size, and a default page size that
/// exceeds the maximum.
/// </summary>
internal sealed class LatticeIdentityDirectoryOptionsValidator : IValidateOptions<LatticeIdentityDirectoryOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeIdentityDirectoryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.DefaultPageSize <= 0)
        {
            failures.Add($"{nameof(LatticeIdentityDirectoryOptions.DefaultPageSize)} must be strictly positive.");
        }

        if (options.MaxPageSize <= 0)
        {
            failures.Add($"{nameof(LatticeIdentityDirectoryOptions.MaxPageSize)} must be strictly positive.");
        }

        if (options.DefaultPageSize > options.MaxPageSize)
        {
            failures.Add(
                $"{nameof(LatticeIdentityDirectoryOptions.DefaultPageSize)} must not exceed " +
                $"{nameof(LatticeIdentityDirectoryOptions.MaxPageSize)}.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
