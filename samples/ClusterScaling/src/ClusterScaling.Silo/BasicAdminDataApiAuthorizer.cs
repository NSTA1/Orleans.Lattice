using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Samples.ClusterScaling.Silo;

/// <summary>
/// Coarse <see cref="ILatticeDataApiAuthorizer"/> for the write-capable data-API
/// gRPC surface that validates the inbound
/// <c>authorization: Basic base64(user:pass)</c> header against an
/// environment-variable-backed dictionary of salted PBKDF2-SHA256 password hashes
/// (never plaintext), replacing the binding's default
/// <see cref="DenyAllDataApiAuthorizer"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each credential lives in an environment variable named
/// <c><see cref="EnvironmentVariablePrefix"/>&lt;username&gt;</c> whose value is an
/// encoded <c>pbkdf2-sha256$&lt;iterations&gt;$&lt;salt&gt;$&lt;key&gt;</c> hash. In
/// this sample the Azure Container Apps deployment injects that hash as a
/// container-app <b>secret</b> surfaced through the env var, so the plaintext
/// admin password is never baked into an image, stored in the resource group, or
/// passed on a command line. The verification re-derives the presented password
/// with the salt and iteration count embedded in the stored hash and compares in
/// constant time via <see cref="LatticePasswordHash.Verify"/>, exactly as the
/// reference <c>EnvVarCredentialAuthorizer</c> does.
/// </para>
/// <para>
/// Basic-over-cleartext is only safe here because Azure Container Apps terminates
/// TLS at its managed ingress: the credential rides an encrypted HTTP/2 channel
/// from the client to the ingress, and the container is reachable only through
/// that ingress. The coarse gate runs first; every mutation still routes through
/// the gated <see cref="ILattice"/> surface so per-tree enforcement (when a host
/// wires <c>AddLatticeAuth</c>) applies afterwards.
/// </para>
/// </remarks>
internal sealed class BasicAdminDataApiAuthorizer : ILatticeDataApiAuthorizer
{
    /// <summary>
    /// The environment-variable name prefix each credential hash is published
    /// under. The username follows the prefix, so <c>admin</c> reads from
    /// <c>LATTICE_DATA_USER_admin</c>. Matches the <c>LATTICE_*_USER_&lt;name&gt;</c>
    /// convention the reference credential-generation scripts under <c>tools/</c>
    /// use.
    /// </summary>
    public const string EnvironmentVariablePrefix = "LATTICE_DATA_USER_";

    private const string AuthorizationHeaderName = "authorization";
    private const string BasicScheme = "Basic ";

    // A well-formed dummy hash so an unknown username spends the same PBKDF2 cost
    // as a real one, keeping response timing from revealing whether a user exists.
    private static readonly string DummyHash =
        LatticePasswordHash.Hash("not-a-real-password-placeholder", LatticePasswordHash.DefaultIterations);

    private readonly ILogger<BasicAdminDataApiAuthorizer> _logger;

    /// <summary>Initialises the authorizer.</summary>
    /// <param name="logger">The logger.</param>
    public BasicAdminDataApiAuthorizer(ILogger<BasicAdminDataApiAuthorizer> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(
        LatticeDataApiAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
    {
        var header = authorizationContext.Call.RequestHeaders.GetValue(AuthorizationHeaderName);
        return Task.FromResult(Authorize(header));
    }

    /// <summary>
    /// Validates an <c>authorization</c> header value (for example
    /// <c>Basic dXNlcjpwYXNz</c>) against the credential dictionary. Exposed for
    /// unit testing without constructing a gRPC <see cref="ServerCallContext"/>.
    /// </summary>
    /// <param name="authorizationHeader">The raw header value, or <see langword="null"/> when absent.</param>
    /// <returns><see langword="true"/> when the credential is valid.</returns>
    internal bool Authorize(string? authorizationHeader)
    {
        if (!TryDecodeBasic(authorizationHeader, out var username, out var password))
        {
            _logger.LogWarning(
                "ClusterScaling: rejected data-API call - missing or malformed Basic authorization header.");
            return false;
        }

        if (!IsValidUsername(username))
        {
            _logger.LogWarning(
                "ClusterScaling: rejected data-API call - username is not a valid environment-variable name.");
            return false;
        }

        var encodedHash = Environment.GetEnvironmentVariable(EnvironmentVariablePrefix + username);
        if (encodedHash is null)
        {
            // Unknown user: spend the equivalent verification cost against a dummy
            // hash so response timing does not reveal whether the user exists.
            _ = LatticePasswordHash.Verify(password, DummyHash);
            _logger.LogWarning("ClusterScaling: rejected data-API call - unknown username.");
            return false;
        }

        if (LatticePasswordHash.Verify(password, encodedHash))
        {
            return true;
        }

        _logger.LogWarning("ClusterScaling: rejected data-API call - incorrect password.");
        return false;
    }

    private static bool TryDecodeBasic(string? header, out string username, out string password)
    {
        username = string.Empty;
        password = string.Empty;

        if (string.IsNullOrWhiteSpace(header) ||
            !header.StartsWith(BasicScheme, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var encoded = header[BasicScheme.Length..].Trim();
        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
        catch (FormatException)
        {
            return false;
        }

        var separator = decoded.IndexOf(':', StringComparison.Ordinal);
        if (separator < 0)
        {
            return false;
        }

        username = decoded[..separator];
        password = decoded[(separator + 1)..];
        return username.Length > 0;
    }

    private static bool IsValidUsername(string username)
    {
        if (username.Length == 0)
        {
            return false;
        }

        var first = username[0];
        if (!char.IsAsciiLetter(first) && first != '_')
        {
            return false;
        }

        foreach (var c in username)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }
}
