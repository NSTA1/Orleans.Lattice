using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Validates <see cref="LatticeAppsOptions"/>: both startup retry delays must be positive and
/// the maximum must not be below the initial delay.
/// </summary>
internal sealed class LatticeAppsOptionsValidator : IValidateOptions<LatticeAppsOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeAppsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        List<string>? failures = null;
        if (options.StartupRetryDelay <= TimeSpan.Zero)
        {
            (failures ??= []).Add($"{nameof(LatticeAppsOptions.StartupRetryDelay)} must be positive.");
        }

        if (options.StartupRetryMaxDelay <= TimeSpan.Zero)
        {
            (failures ??= []).Add($"{nameof(LatticeAppsOptions.StartupRetryMaxDelay)} must be positive.");
        }
        else if (options.StartupRetryMaxDelay < options.StartupRetryDelay)
        {
            (failures ??= []).Add(
                $"{nameof(LatticeAppsOptions.StartupRetryMaxDelay)} must not be less than {nameof(LatticeAppsOptions.StartupRetryDelay)}.");
        }

        return failures is null ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}
