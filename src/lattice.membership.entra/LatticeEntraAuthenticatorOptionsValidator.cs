using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Validates <see cref="LatticeEntraAuthenticatorOptions"/>: an authority is
/// required, at least one tenant id, one audience and one signature algorithm
/// must be configured, the issuer template must carry the <c>{tenantid}</c>
/// placeholder, the group-resolution mode must be a defined value, and the
/// refresh intervals must be strictly positive.
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

        // Fail loud, at startup, rather than fail closed silently at every
        // authentication. An empty pin is refused here because the alternative -
        // reaching EntraCredentialAuthenticator's deny-all branch - denies every
        // token at runtime with nothing naming the option responsible, which
        // presents as a total authentication outage rather than as the
        // configuration error it is. That runtime branch is deliberately kept as
        // defence in depth for the direct-construction path this validator does
        // not sit on; it is not made unreachable by this check.
        if (options.Algorithms.Count == 0)
        {
            failures.Add(
                $"{nameof(LatticeEntraAuthenticatorOptions.Algorithms)} must contain at least one signature algorithm. " +
                "An empty allow-list is refused rather than treated as 'accept any algorithm' (CWE-347); " +
                $"clear and repopulate it to accept a set other than the default '{LatticeEntraAuthenticatorOptions.DefaultAlgorithm}'.");
        }
        else if (options.Algorithms.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeEntraAuthenticatorOptions.Algorithms)} must not contain a null or empty algorithm.");
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
