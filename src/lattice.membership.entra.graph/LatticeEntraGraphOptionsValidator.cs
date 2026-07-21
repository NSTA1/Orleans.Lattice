using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Validates <see cref="LatticeEntraGraphOptions"/>: exactly one authentication
/// mode must be selected - either a secret-less <see cref="LatticeEntraGraphOptions.Credential"/>
/// or the complete confidential-client triple (tenant id, client id, and client
/// secret) - at least one scope must be configured, and the token refresh skew
/// must not be negative.
/// </summary>
internal sealed class LatticeEntraGraphOptionsValidator : IValidateOptions<LatticeEntraGraphOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeEntraGraphOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        var hasCredential = options.Credential is not null;
        var hasSecret = !string.IsNullOrWhiteSpace(options.ClientSecret);
        var hasTenant = !string.IsNullOrWhiteSpace(options.TenantId);
        var hasClientId = !string.IsNullOrWhiteSpace(options.ClientId);

        // Fail closed on an ambiguous configuration: the two authentication modes
        // are mutually exclusive, so a Credential paired with a client secret is a
        // misconfiguration we reject rather than silently pick one.
        if (hasCredential && hasSecret)
        {
            failures.Add(
                $"{nameof(LatticeEntraGraphOptions.Credential)} and {nameof(LatticeEntraGraphOptions.ClientSecret)} " +
                "are mutually exclusive: supply a Credential for the secret-less path or a ClientSecret for the " +
                "confidential-client path, not both.");
        }
        else if (hasCredential)
        {
            // Secret-less path: the credential authenticates app-only; tenant id,
            // client id, and client secret are not used and are not required.
        }
        else if (hasSecret)
        {
            // Confidential-client path: the full triple is required.
            if (!hasTenant)
            {
                failures.Add($"{nameof(LatticeEntraGraphOptions.TenantId)} must be set for the client-secret path.");
            }

            if (!hasClientId)
            {
                failures.Add($"{nameof(LatticeEntraGraphOptions.ClientId)} must be set for the client-secret path.");
            }
        }
        else
        {
            // Fail closed: neither authentication mode is fully configured.
            failures.Add(
                $"An authentication mode must be configured: set {nameof(LatticeEntraGraphOptions.Credential)} for " +
                $"the secret-less path, or {nameof(LatticeEntraGraphOptions.TenantId)}, " +
                $"{nameof(LatticeEntraGraphOptions.ClientId)}, and {nameof(LatticeEntraGraphOptions.ClientSecret)} " +
                "for the confidential-client path.");
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
