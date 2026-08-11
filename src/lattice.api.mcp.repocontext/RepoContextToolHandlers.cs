using System.ComponentModel;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The adapter behind the repository-context tool module. It exposes the
/// read-only <c>repocontext_health</c> probe - which proves the module is
/// registered and that the caller cleared the fail-closed authorization gate -
/// and the mutating <c>repocontext_bootstrap</c> onboarding tool that ingests a
/// codebase into the context store. The capture, maintenance, and retrieval
/// handlers land in later work.
/// </summary>
/// <remarks>
/// The health result is invariant, so it is built once and reused on every call:
/// the probe adds no per-invocation allocation to the hot path. The bootstrap
/// handler resolves its coordinator from the request service provider and adds no
/// authorization path of its own - the fail-closed gate that advertises the
/// mutating tool only to a write-opted-in caller is inherited from the discovery
/// core.
/// </remarks>
internal static class RepoContextToolHandlers
{
    /// <summary>
    /// The single, shared health result. It carries no caller- or
    /// request-specific state, so one immutable instance serves every session and
    /// no allocation occurs per <c>tools/call</c>.
    /// </summary>
    private static readonly RepoContextHealthResult Healthy = new()
    {
        Available = true,
        Group = LatticeApiMcpGroupCapabilityMap.DisplayName(LatticeApiMcpGroup.RepoContext),
        Status = "The Orleans.Lattice repository-context MCP surface is registered and reachable.",
    };

    /// <summary>
    /// Reports that the repository-context surface is available to the caller.
    /// Reaching this handler means the caller was advertised the tool and cleared
    /// the authorization gate, so it always returns the ready result.
    /// </summary>
    /// <returns>The shared, immutable health result.</returns>
    public static RepoContextHealthResult Health() => Healthy;

    /// <summary>
    /// Ingests a repository working tree into the context store: walks the tree,
    /// records a structural node and content digest per file, and reconciles the
    /// scan against the stored records idempotently (unchanged files are skipped,
    /// changed files updated, and deleted files pruned). Reaching this handler
    /// means the caller cleared the fail-closed authorization gate and the host
    /// opted writes in.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the
    /// bootstrap coordinator from the request service provider.</param>
    /// <param name="repoRoot">The absolute path to the repository working tree the
    /// server should walk.</param>
    /// <param name="repoId">The stable repository identity records are filed under.</param>
    /// <param name="includeGlobs">Optional include globs; when non-empty a file is
    /// ingested only if it matches at least one.</param>
    /// <param name="excludeGlobs">Optional exclude globs; a match removes a file
    /// from the walk even when it also matched an include.</param>
    /// <param name="cancellationToken">Cancels the ingestion run.</param>
    /// <returns>A summary of files scanned, added, updated, removed, and unchanged.</returns>
    /// <exception cref="McpException">A required argument is missing or the
    /// repository root does not exist (caller errors).</exception>
    public static async Task<RepoContextBootstrapResult> BootstrapAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Absolute path to the repository working tree the server should walk and ingest.")]
        string repoRoot,
        [Description("Stable repository identity to file records under, so re-ingesting the same codebase updates the same records.")]
        string repoId,
        [Description("Optional include globs (for example 'src/**' or '*.cs'); when non-empty a file is ingested only if it matches at least one. '**' matches any depth, '*' a single path segment.")]
        IReadOnlyList<string>? includeGlobs = null,
        [Description("Optional exclude globs; a match removes a file even when it also matched an include. The '.git' directory is always skipped.")]
        IReadOnlyList<string>? excludeGlobs = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoRoot))
        {
            throw new McpException("The 'repoRoot' parameter is required and must be a non-empty path.");
        }

        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the bootstrap tool cannot resolve its coordinator.");
        var coordinator = services.GetRequiredService<RepoContextBootstrapService>();

        var request = new RepoContextBootstrapRequest
        {
            RepoRoot = repoRoot,
            RepoId = repoId,
            IncludeGlobs = includeGlobs,
            ExcludeGlobs = excludeGlobs,
        };

        try
        {
            return await coordinator.RunAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (DirectoryNotFoundException ex)
        {
            throw new McpException(ex.Message);
        }
    }
}
