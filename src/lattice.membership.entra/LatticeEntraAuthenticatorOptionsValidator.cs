using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Validates <see cref="LatticeEntraAuthenticatorOptions"/>: an authority is
/// required, at least one tenant id and one audience must be configured, the
/// issuer template must carry the <c>{tenantid}</c> placeholder, the
/// group-resolution mode must be a defined value, and the refresh intervals must
/// be strictly positive.
/// </summary>
internal sealed class LatticeEntraAuthenticatorOptionsValidator : IValidateOptions<LatticeEntraAuthenticatorOptions>
{
    /// <summary>The placeholder the issuer template must contain.</summary>
    internal const string TenantPlaceholder = "{tenantid}";

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeEntraAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.Authority))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.Authority)} must be set.");
        }

        if (options.TenantIds.Count == 0)
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.TenantIds)} must contain at least one tenant id.");
        }
        else if (options.TenantIds.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.TenantIds)} must not contain a null or empty tenant id.");
        }

        if (options.Audiences.Count == 0)
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.Audiences)} must contain at least one audience.");
        }
        else if (options.Audiences.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.Audiences)} must not contain a null or empty audience.");
        }

        if (string.IsNullOrWhiteSpace(options.IssuerTemplate) ||
            !options.IssuerTemplate.Contains(TenantPlaceholder, StringComparison.Ordinal))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.IssuerTemplate)} must contain the '{TenantPlaceholder}' placeholder.");
        }

        if (!Enum.IsDefined(options.GroupResolutionMode))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.GroupResolutionMode)} must be a defined EntraGroupResolutionMode value.");
        }

        if (options.AutomaticRefreshInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.AutomaticRefreshInterval)} must be strictly positive.");
        }

        if (options.RefreshInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.RefreshInterval)} must be strictly positive.");
        }

        if (options.ClockSkew < TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.ClockSkew)} must not be negative.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Validates <paramref name="options"/> and throws when invalid. Used at
    /// registration to fail fast with an actionable message.
    /// </summary>
    /// <param name="options">The options to validate. Must not be <c>null</c>.</param>
    /// <exception cref="OptionsValidationException">The options are invalid.</exception>
    internal static void ValidateAndThrow(LatticeEntraAuthenticatorOptions options)
    {
        var result = new LatticeEntraAuthenticatorOptionsValidator().Validate(Options.DefaultName, options);
        if (result.Failed)
        {
            throw new OptionsValidationException(
                Options.DefaultName,
                typeof(LatticeEntraAuthenticatorOptions),
                result.Failures);
        }
    }
}
