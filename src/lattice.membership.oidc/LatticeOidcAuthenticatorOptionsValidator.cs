using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// Validates <see cref="LatticeOidcAuthenticatorOptions"/>: an authority and an
/// exact issuer are required, at least one audience and one subject claim type
/// must be configured, no configured audience, claim type, or algorithm may be
/// blank, the refresh intervals must be strictly positive, and the clock skew
/// must not be negative.
/// </summary>
internal sealed class LatticeOidcAuthenticatorOptionsValidator : IValidateOptions<LatticeOidcAuthenticatorOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeOidcAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.Authority))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.Authority)} must be set.");
        }

        if (string.IsNullOrWhiteSpace(options.Issuer))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.Issuer)} must be set.");
        }

        if (options.Audiences.Count == 0)
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.Audiences)} must contain at least one audience.");
        }
        else if (options.Audiences.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.Audiences)} must not contain a null or empty audience.");
        }

        if (options.SubjectClaimTypes.Count == 0)
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.SubjectClaimTypes)} must contain at least one claim type.");
        }
        else if (options.SubjectClaimTypes.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.SubjectClaimTypes)} must not contain a null or empty claim type.");
        }

        if (options.GroupClaimTypes.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.GroupClaimTypes)} must not contain a null or empty claim type.");
        }

        if (options.Algorithms.Any(string.IsNullOrWhiteSpace))
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.Algorithms)} must not contain a null or empty algorithm.");
        }

        if (options.AutomaticRefreshInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.AutomaticRefreshInterval)} must be strictly positive.");
        }

        if (options.RefreshInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.RefreshInterval)} must be strictly positive.");
        }

        if (options.ClockSkew < TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeOidcAuthenticatorOptions.ClockSkew)} must not be negative.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Validates <paramref name="options"/> and throws when invalid. Called from
    /// the authenticator factory the registration extension registers, so an
    /// invalid configuration fails with an actionable message the first time the
    /// authenticator is resolved from the container rather than at registration.
    /// </summary>
    /// <param name="options">The options to validate. Must not be <c>null</c>.</param>
    /// <exception cref="OptionsValidationException">The options are invalid.</exception>
    internal static void ValidateAndThrow(LatticeOidcAuthenticatorOptions options)
    {
        var result = new LatticeOidcAuthenticatorOptionsValidator().Validate(Options.DefaultName, options);
        if (result.Failed)
        {
            throw new OptionsValidationException(
                Options.DefaultName,
                typeof(LatticeOidcAuthenticatorOptions),
                result.Failures);
        }
    }
}
