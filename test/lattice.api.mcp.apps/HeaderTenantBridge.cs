using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>An active-tenant bridge that reads the asserted tenant from the <c>x-tenant</c> request header.</summary>
internal sealed class HeaderTenantBridge : ILatticeApiMcpActiveTenantBridge
{
    public const string HeaderName = "x-tenant";

    public TenantId? Resolve(HttpContext context)
        => context.Request.Headers.TryGetValue(HeaderName, out var value) && TenantId.TryParse(value.ToString(), out var tenant)
            ? tenant
            : null;
}
