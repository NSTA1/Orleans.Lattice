using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Validates <see cref="LatticeEntraGraphOptions"/>: tenant id, client id, and
/// client secret are required, at least one scope must be configured, and the
/// token refresh skew must not be negative.
/// </summary>
internal sealed class LatticeEntraGraphOptionsValidator : IValidateOptions<LatticeEntraGraphOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeEntraGraphOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.TenantId))
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.TenantId)} must be set.");
        }

        if (string.IsNullOrWhiteSpace(options.ClientId))
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.ClientId)} must be set.");
        }

        if (string.IsNullOrWhiteSpace(options.ClientSecret))
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.ClientSecret)} must be set.");
        }

        if (options.Scopes.Count == 0)
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.Scopes)} must contain at least one scope.");
        }
        else if (options.Scopes.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.Scopes)} must not contain a null or empty scope.");
        }

        if (options.TokenRefreshSkew < TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeEntraGraphOptions.TokenRefreshSkew)} must not be negative.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Validates <paramref name="options"/> and throws when invalid.
    /// </summary>
    /// <param name="options">The options to validate. Must not be <c>null</c>.</param>
    /// <exception cref="OptionsValidationException">The options are invalid.</exception>
    internal static void ValidateAndThrow(LatticeEntraGraphOptions options)
    {
        var result = new LatticeEntraGraphOptionsValidator().Validate(Options.DefaultName, options);
        if (result.Failed)
        {
            throw new OptionsValidationException(
                Options.DefaultName,
                typeof(LatticeEntraGraphOptions),
                result.Failures);
        }
    }
}
