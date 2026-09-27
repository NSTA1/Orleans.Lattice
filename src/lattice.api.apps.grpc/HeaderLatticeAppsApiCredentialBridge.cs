using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class HeaderLatticeAppsApiCredentialBridge(
    IOptions<LatticeAppsApiGrpcOptions> options) : ILatticeAppsApiCredentialBridge
{
    private readonly LatticeAppsApiGrpcOptions _options =
        (options ?? throw new ArgumentNullException(nameof(options))).Value;
    private readonly string? _headerName = options.Value.CredentialHeaderName?.ToLowerInvariant();

    public LatticeCredential? Resolve(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (string.IsNullOrEmpty(_headerName))
            return null;

        var raw = context.RequestHeaders.GetValue(_headerName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var token = raw.AsSpan().Trim();
        var scheme = _options.CredentialScheme;
        if (!string.IsNullOrEmpty(scheme)
            && token.StartsWith(scheme, StringComparison.OrdinalIgnoreCase)
            && (token.Length == scheme.Length || char.IsWhiteSpace(token[scheme.Length])))
            token = token[scheme.Length..].Trim();

        return token.IsEmpty ? null : new LatticeCredential(
            token.Length == raw.Length ? raw : token.ToString(), string.IsNullOrEmpty(scheme) ? null : scheme);
    }
}
