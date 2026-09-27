using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The additive discovery seam an installable-app tool surface plugs into so the
/// permission-aware discovery core can advertise app-contributed tools per
/// session without widening the enum-keyed <see cref="ILatticeApiMcpToolGroup"/>
/// seam. The discovery core consults every registered source after the group
/// tools, for an authenticated caller only, and adds each returned tool through
/// the same per-session collection path the group tools use: the coarse
/// <see cref="ILatticeApiMcpAuthorizer"/> gate at advertisement, and a
/// <see cref="CredentialStampingTool"/> wrapper (credential and active-tenant
/// stamping, strict argument binding, fault translation, and current-region-only
/// routing) at invocation.
/// </summary>
/// <remarks>
/// <para>
/// A source owns the per-tool permission decision: it returns only the tools the
/// caller may use. Because the session's tool collection serves both
/// <c>tools/list</c> and <c>tools/call</c>, a tool a source withholds is
/// unreachable at invocation too.
/// </para>
/// <para>
/// With no source registered the discovery core behaves byte-for-byte as before:
/// the advertised tool list, the <c>lattice_capabilities</c> report, and the
/// server instructions are unchanged. App tools never appear in the capability
/// report, which describes facade groups only.
/// </para>
/// <para>
/// Implementations must precompute their tool instances and select from prebuilt
/// lists per session rather than re-materialising a tool per <c>tools/list</c>.
/// A name that collides with a tool already in the session (a built-in meta-tool
/// or a group tool) is skipped with a warning, so an app can never shadow a
/// built-in tool.
/// </para>
/// </remarks>
internal interface ILatticeApiMcpAppToolSource
{
    /// <summary>
    /// Returns the app tools <paramref name="credential"/> may use in the session
    /// initiated by <paramref name="httpContext"/>, each already carrying its
    /// advertised (namespaced) name.
    /// </summary>
    /// <param name="httpContext">The request context that initiated the session.</param>
    /// <param name="credential">The caller's resolved credential.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The permitted tools; empty when none are permitted.</returns>
    /// <exception cref="LatticeApiMcpDiscoveryUnavailableException">
    /// A backend the decision depends on was transiently unreachable, so no
    /// authoritative answer exists; the session plan surfaces a retryable error.
    /// </exception>
    ValueTask<IReadOnlyList<McpServerTool>> GetPermittedToolsAsync(
        HttpContext httpContext,
        LatticeCredential credential,
        CancellationToken cancellationToken);
}
