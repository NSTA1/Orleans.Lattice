using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Default <see cref="ILatticeReplicationApiCredentialBridge"/> that lifts a
/// single configurable request header into a <see cref="LatticeCredential"/>.
/// Reads the header named by
/// <see cref="LatticeReplicationApiGrpcOptions.CredentialHeaderName"/> (default
/// <c>authorization</c>), strips a leading
/// <see cref="LatticeReplicationApiGrpcOptions.CredentialScheme"/> prefix
/// (default <c>Bearer</c>) when present, and stamps the remaining token as the
/// credential with that scheme so a registered
/// <c>ILatticeCredentialAuthenticator</c> can resolve the caller's subject.
/// </summary>
/// <remarks>
/// A registered <c>ILatticeCredentialAuthenticator</c> is responsible for
/// validating the token; this bridge performs no validation and only shuttles
/// the opaque token onto the ambient credential context. An absent, empty, or
/// whitespace header yields <see langword="null"/> (an anonymous caller), which
/// auth-backed replication control fails closed.
/// </remarks>
internal sealed class HeaderLatticeReplicationApiCredentialBridge : ILatticeReplicationApiCredentialBridge
{
    private readonly IOptions<LatticeReplicationApiGrpcOptions> _options;

    /// <summary>
    /// Initialises the bridge with the resolved gRPC binding options.
    /// </summary>
    public HeaderLatticeReplicationApiCredentialBridge(IOptions<LatticeReplicationApiGrpcOptions> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public LatticeCredential? Resolve(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var options = _options.Value;
        var headerName = options.CredentialHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        // gRPC metadata keys are stored lower-cased; normalise the lookup so a
        // configured header name with any casing matches the inbound entry.
        var raw = context.RequestHeaders?.GetValue(headerName.ToLowerInvariant());
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var scheme = options.CredentialScheme;
        var token = raw.Trim();
        if (!string.IsNullOrEmpty(scheme)
            && token.Length >= scheme.Length
            && token.AsSpan(0, scheme.Length).Equals(scheme, StringComparison.OrdinalIgnoreCase)
            && (token.Length == scheme.Length || char.IsWhiteSpace(token[scheme.Length])))
        {
            // A bare scheme with no token (for example "Bearer ") is not a
            // credential; collapse it to empty so the caller reads as anonymous.
            token = token.Length == scheme.Length
                ? string.Empty
                : token[(scheme.Length + 1)..].Trim();
        }

        return string.IsNullOrEmpty(token)
            ? null
            : new LatticeCredential(token, string.IsNullOrEmpty(scheme) ? null : scheme);
    }
}
