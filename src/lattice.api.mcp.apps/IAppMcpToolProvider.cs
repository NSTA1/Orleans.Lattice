using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The seam app code implements to contribute the MCP tool implementations its
/// manifest declares. Register one or more implementations in DI (for example
/// <c>services.AddSingleton&lt;IAppMcpToolProvider, MyAppTools&gt;()</c>); the app tool
/// surface pairs them with the enabled app's manifest and advertises each tool as
/// <c>{slug}_{tool}</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>App-local names.</b> Each tool in <see cref="Tools"/> is named by its app-local
/// name - the <see cref="AppMcpToolDeclaration.Name"/> it implements - never by the
/// namespaced name. The surface applies the <c>{slug}_</c> prefix itself, so two apps
/// may reuse the same local name without colliding.
/// </para>
/// <para>
/// <b>Exact pairing or nothing.</b> For every enabled app the surface pairs the
/// manifest's declarations with the union of every provider registered for the app's
/// <see cref="Slug"/>. A declared tool with no implementation, an implementation the
/// manifest does not declare, or a local name implemented twice fails the app's tool
/// activation as a whole: the app contributes no tools at all and the failure is
/// logged. First-wins registration would let a second contribution shadow the first.
/// </para>
/// <para>
/// <b>Prebuilt and stateless.</b> <see cref="Tools"/> is read when the app's tool
/// activation is built, not per session, so an implementation should build its tools
/// once. Tools resolve their collaborators from the request service provider and run
/// under the calling session's credential, so the shared access gate authorizes every
/// data-plane call they make against the real caller. An existing package can adapt its
/// <see cref="McpServerTool"/> instances by returning them here under their app-local
/// names.
/// </para>
/// </remarks>
public interface IAppMcpToolProvider
{
    /// <summary>The app whose declared tools this provider implements.</summary>
    AppSlug Slug { get; }

    /// <summary>The tool implementations, each named by its app-local name.</summary>
    IReadOnlyList<McpServerTool> Tools { get; }
}
