using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context tool module: the <see cref="ILatticeApiMcpToolGroup"/>
/// for <see cref="LatticeApiMcpGroup.RepoContext"/>. It plugs the companion
/// repository-context package into the <c>Orleans.Lattice.Api.Mcp</c> binding's
/// permission-aware discovery core so a caller holding a data read-or-write grant
/// is offered the <c>repocontext_*</c> tools scoped exactly like the built-in
/// groups (per-session discovery, region routing, and the fail-closed
/// authorization gate all come from the core seam - this module adds none of its
/// own).
/// </summary>
/// <remarks>
/// <para>
/// The group contributes a read-only <c>repocontext_health</c> probe that proves
/// the registration and the authorization gate work end to end, and - when the
/// host opts writes in - the mutating <c>repocontext_bootstrap</c> onboarding tool
/// that ingests a codebase into the context store. The capture, maintenance, and
/// retrieval tools land in later work.
/// </para>
/// <para>
/// The tool list is built <b>once</b> in the constructor, so the per-session
/// discovery filter selects from this prebuilt list and never re-materialises a
/// tool per <c>tools/list</c> or <c>tools/call</c>.
/// </para>
/// </remarks>
internal sealed class RepoContextToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the repository-context tool list once. When
    /// <paramref name="enableWrites"/> is <see langword="false"/> (the default)
    /// only the read-only <c>repocontext_health</c> probe is contributed; when
    /// <see langword="true"/> the mutating <c>repocontext_bootstrap</c> tool is
    /// added. The tools resolve any collaborators from the request service
    /// provider at call time, so no per-session state is captured here.
    /// </summary>
    /// <param name="enableWrites">Whether the mutating repository-context tools are
    /// contributed.</param>
    public RepoContextToolGroup(bool enableWrites = false)
    {
        var tools = new List<McpServerTool>(enableWrites ? 2 : 1)
        {
            McpServerTool.Create(
                RepoContextToolHandlers.Health,
                new McpServerToolCreateOptions
                {
                    Name = "repocontext_health",
                    Title = "Repository-context health",
                    Description =
                        "Reports whether the Orleans.Lattice repository-context surface is registered and "
                        + "reachable for the current authenticated caller. Returns success only when the caller "
                        + "cleared the fail-closed authorization gate, so an agent can confirm the surface is "
                        + "wired end to end before attempting the capture, maintenance, and retrieval tools that "
                        + "land in later work. Read-only.",
                    ReadOnly = true,
                    Destructive = false,
                    UseStructuredContent = true,
                }),
        };

        if (enableWrites)
        {
            tools.Add(BuildBootstrapTool());
        }

        Tools = tools;
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.RepoContext;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static McpServerTool BuildBootstrapTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.BootstrapAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_bootstrap",
                Title = "Bootstrap a repository into the context store",
                Description =
                    "Onboards a codebase into the repository-context store so an agent starts from a "
                    + "populated, queryable baseline instead of empty memory. Walks the repository at "
                    + "'repoRoot', records a structural node and content digest for every file under the "
                    + "'repoId' keyspace, and reconciles the scan against the stored records. Idempotent and "
                    + "resumable: re-running on an unchanged repository is a no-op, a changed repository "
                    + "updates only changed files and prunes deleted ones, and an interrupted run resumes "
                    + "without duplication. Returns a summary of files scanned, added, updated, removed, and "
                    + "unchanged, symbols captured, and elapsed time. Fails closed: offered only to a caller "
                    + "who cleared the authorization gate and for whom the host opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });
}
