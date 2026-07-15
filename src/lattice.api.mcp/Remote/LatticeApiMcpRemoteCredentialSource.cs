using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The remote-host <see cref="ILatticeApiMcpRemoteCredentialSource"/>. Selects
/// the credential to forward to the remote cluster per outbound gRPC call so the
/// remote cluster enforces the same fail-closed, permission-scoped behaviour as
/// an in-silo binding.
/// </summary>
/// <remarks>
/// <para>Resolution order, first match wins:</para>
/// <list type="number">
/// <item>
/// When the current turn is a trusted system-origin introspection
/// (<see cref="LatticeSystemOrigin.IsActive"/>, entered by the
/// discovery core to resolve a caller's effective permissions) and an
/// <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/> is
/// configured, the administrator service credential is forwarded. This is
/// required because the remote auth cluster re-runs its own administrator gate;
/// the in-silo bypass does not cross the wire.
/// </item>
/// <item>
/// Otherwise the ambient <see cref="LatticeCredentialContext.Current"/> credential
/// if one is stamped (for example by the backup tool module).
/// </item>
/// <item>
/// Otherwise the caller credential the MCP credential bridge resolves from the
/// ambient HTTP request, which is how the state, data, and auth tool modules
/// present the caller identity (they do not stamp the ambient context).
/// </item>
/// </list>
/// <para>
/// When none resolves, the call is anonymous and the remote cluster fails closed.
/// </para>
/// </remarks>
internal sealed class LatticeApiMcpRemoteCredentialSource : ILatticeApiMcpRemoteCredentialSource
{
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly ILatticeApiMcpCredentialBridge _credentialBridge;
    private readonly IOptionsMonitor<LatticeApiMcpRemoteOptions> _options;

    /// <summary>Initialises the credential source from the ambient HTTP accessor, bridge, and options.</summary>
    public LatticeApiMcpRemoteCredentialSource(
        IHttpContextAccessor httpContextAccessor,
        ILatticeApiMcpCredentialBridge credentialBridge,
        IOptionsMonitor<LatticeApiMcpRemoteOptions> options)
    {
        _httpContextAccessor = httpContextAccessor ?? throw new ArgumentNullException(nameof(httpContextAccessor));
        _credentialBridge = credentialBridge ?? throw new ArgumentNullException(nameof(credentialBridge));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public LatticeCredential? ResolveOutbound()
    {
        // Trusted permission introspection: forward the configured administrator
        // service credential so the remote auth gate admits the read. Falls
        // through to the caller-credential path when no service credential is
        // configured, so an administrator caller can still introspect itself.
        if (LatticeSystemOrigin.IsActive)
        {
            var administrator = _options.CurrentValue.AdministratorCredential;
            if (administrator is not null)
            {
                return administrator;
            }
        }

        var ambient = LatticeCredentialContext.Current;
        if (ambient is not null)
        {
            return ambient;
        }

        var httpContext = _httpContextAccessor.HttpContext;
        return httpContext is null ? null : _credentialBridge.Resolve(httpContext);
    }
}
