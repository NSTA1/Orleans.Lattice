using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Validates <see cref="LatticeApiMcpManagedIdentityAdministratorOptions"/> at
/// host start: requires an Azure credential, a non-empty scope, and a
/// non-negative refresh skew.
/// </summary>
internal sealed class LatticeApiMcpManagedIdentityAdministratorOptionsValidator
    : IValidateOptions<LatticeApiMcpManagedIdentityAdministratorOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(
        string? name,
        LatticeApiMcpManagedIdentityAdministratorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.Credential is null)
        {
            failures.Add(
                $"{nameof(LatticeApiMcpManagedIdentityAdministratorOptions.Credential)} must be supplied "
                + "(for example new ManagedIdentityCredential()).");
        }

        if (string.IsNullOrWhiteSpace(options.Scope))
        {
            failures.Add(
                $"{nameof(LatticeApiMcpManagedIdentityAdministratorOptions.Scope)} must be a non-empty scope "
                + "(the remote silo audience, for example api://<silo-app-id>/.default).");
        }

        if (options.RefreshSkew < TimeSpan.Zero)
        {
            failures.Add(
                $"{nameof(LatticeApiMcpManagedIdentityAdministratorOptions.RefreshSkew)} must not be negative.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
