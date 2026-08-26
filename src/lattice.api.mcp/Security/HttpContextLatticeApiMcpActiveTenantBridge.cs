using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Default <see cref="ILatticeApiMcpActiveTenantBridge"/> that lifts a single
/// configurable request header into a <see cref="TenantId"/>. Reads the header
/// named by <see cref="LatticeApiMcpOptions.ActiveTenantHeaderName"/> (default
/// <c>lattice-active-tenant</c>) and parses its value against the tenant id
/// grammar, returning the parsed tenant so the MCP tool invocation seam can stamp
/// it as the call's ambient active tenant.
/// </summary>
/// <remarks>
/// The asserted tenant is validated against the caller's subject membership by the
/// tenancy add-on downstream; this bridge performs no authorization and only
/// shuttles a syntactically valid tenant assertion onto the ambient scope. An
/// absent, empty, whitespace, or syntactically invalid header yields
/// <see langword="null"/> (no active tenant asserted), which the resolver treats
/// fail-closed. The header is read regardless of whether the session is
/// authenticated; the credential seam - not this one - decides caller identity.
/// </remarks>
internal sealed class HttpContextLatticeApiMcpActiveTenantBridge : ILatticeApiMcpActiveTenantBridge
{
    private readonly IOptions<LatticeApiMcpOptions> _options;

    /// <summary>
    /// Initialises the bridge with the resolved MCP binding options.
    /// </summary>
    public HttpContextLatticeApiMcpActiveTenantBridge(IOptions<LatticeApiMcpOptions> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public TenantId? Resolve(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var headerName = _options.Value.ActiveTenantHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        var raw = context.Request.Headers[headerName].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        // Fail-closed: a header that is not a syntactically valid tenant id is not
        // an assertion we honour - the call proceeds with no active tenant.
        return TenantId.TryParse(raw.Trim(), out var tenant) ? tenant : null;
    }
}
