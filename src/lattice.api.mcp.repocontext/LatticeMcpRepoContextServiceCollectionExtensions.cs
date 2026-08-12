using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context tool-module opt-in for the <c>Orleans.Lattice.Api.Mcp</c>
/// binding. Adds the MCP tool group that serves an AI agent a durable,
/// conflict-free store of structural facts, notes, and working memory about a
/// codebase.
/// </summary>
/// <remarks>
/// <para>Registered as a companion to <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddRepoContextTools();
/// // ...
/// app.MapLatticeMcp();
/// </code>
/// <para>
/// The permission-aware discovery core advertises the module's tools only to a
/// caller granted the repository-context group (the same data read-or-write mask
/// that makes the data group usable), and the fail-closed authorization gate
/// enforces the verdict at both advertisement and invocation - this module adds
/// no authorization path of its own. The host must also have called
/// <c>AddLatticeMcp</c> for the tools to be reachable.
/// </para>
/// </remarks>
public static class LatticeMcpRepoContextServiceCollectionExtensions
{
    /// <summary>
    /// Opts the repository-context surface into the MCP binding: registers the
    /// repository-context tool group so its tools are advertised to a caller
    /// holding a data read-or-write grant, along with the bootstrap coordinator,
    /// the retrieval services (the exact-kNN semantic index and the search
    /// orchestrator), and the embed-and-store bootstrap vectorisation seam.
    /// Idempotent for the tool group:
    /// calling it more than once registers exactly one tool group. The host must
    /// also have called <c>AddLatticeMcp</c> for the tools to be reachable.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableWrites">
    /// Whether the mutating repository-context tools (the <c>repocontext_bootstrap</c>
    /// onboarding tool and the <c>repocontext_remember</c>, <c>repocontext_update</c>,
    /// and <c>repocontext_forget</c> capture and maintenance tools) are contributed.
    /// Defaults to <see langword="false"/> so a data-permitted caller is offered the
    /// read-only surface only until the host explicitly opts writes in.
    /// </param>
    /// <param name="workspaceMode">
    /// Whether the dynamic multi-repository workspace surface is contributed. When
    /// <see langword="false"/> (the default) the mutating onboarding tool is the
    /// single-repository <c>repocontext_bootstrap</c>. When <see langword="true"/>
    /// the read-only <c>repocontext_list_repos</c> is added and
    /// <c>repocontext_bootstrap</c> is replaced by the workspace-scoped
    /// <c>repocontext_add_repo</c> and <c>repocontext_remove_repo</c>.
    /// </param>
    /// <param name="workspaceRoot">
    /// The read-only workspace root that runtime-added repositories must resolve
    /// under. When supplied, a fail-closed path guard rejects any
    /// <c>repocontext_add_repo</c> path that escapes the root (via <c>..</c> or a
    /// symbolic link). When <see langword="null"/> or empty, a disabled guard is
    /// registered that permits any path (the single-repository default). Ignored
    /// unless a host registers no guard of its own first.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRepoContextTools(
        this IServiceCollection services,
        bool enableWrites = false,
        bool workspaceMode = false,
        string? workspaceRoot = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup>(
                new RepoContextToolGroup(enableWrites, workspaceMode)));

        // Register the workspace guard. A supplied root produces a fail-closed
        // guard that rejects any add-repo path escaping it; an absent root produces
        // a disabled guard that permits any path (the single-repository default).
        // TryAdd means a host (or test harness) that registered its own guard
        // first wins, so the boundary can always be overridden for testing.
        var roots = string.IsNullOrWhiteSpace(workspaceRoot)
            ? Array.Empty<string>()
            : new[] { workspaceRoot };
        services.TryAddSingleton(new RepoContextWorkspaceGuard(roots));

        // Wire the bootstrap-time vectorisation seam to the real embed-and-store
        // ingestor (replacing the deferred no-op): a bootstrap run now embeds the
        // files it added or updated and lands their vectors on the reserved vector
        // trees, so the search tool can find them. The ingestor resolves an
        // IEmbeddingProvider only if the host bound one; absent (or unavailable) it
        // fails closed and search degrades to keyword recall.
        services.TryAddSingleton<IRepoContextVectorIngestor>(sp =>
            new EmbeddingRepoContextVectorIngestor(
                sp.GetRequiredService<RepoContextVectorWriter>(),
                sp.GetRequiredService<ILogger<EmbeddingRepoContextVectorIngestor>>(),
                sp.GetService<IEmbeddingProvider>()));
        services.TryAddSingleton<RepoContextVectorWriter>();
        services.TryAddSingleton<IRepoContextSemanticIndex, ExactKnnSemanticIndex>();
        services.TryAddSingleton<RepoContextSearchService>(sp =>
            new RepoContextSearchService(
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<Orleans.Serialization.Serializer>(),
                sp.GetRequiredService<IRepoContextSemanticIndex>(),
                sp.GetRequiredService<RepoContextStore>(),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<ILogger<RepoContextSearchService>>(),
                sp.GetService<IEmbeddingProvider>()));

        services.TryAddSingleton<RepoContextBootstrapService>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<RepoContextStore>();

        // Bind the per-repository TTL policy under the named-options convention
        // (IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)) and validate every
        // instance at first resolve. The memory-writing tools consume these.
        services.AddOptions<RepoContextTtlOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<RepoContextTtlOptions>, RepoContextTtlOptionsValidator>());

        return services;
    }
}
