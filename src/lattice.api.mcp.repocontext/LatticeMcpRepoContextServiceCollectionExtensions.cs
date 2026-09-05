using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
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
    /// <para>
    /// <b>Required by workspace mode.</b> A disabled guard admits every path, so
    /// <c>repocontext_add_repo</c> - whose path comes from the wire - refuses at
    /// invocation unless the effective guard is enforcing. Supply a root here, or
    /// register an enforcing <c>RepoContextWorkspaceGuard</c> before this call,
    /// whenever <paramref name="workspaceMode"/> is <see langword="true"/>. The
    /// disabled guard remains the intended shape for the single-repository
    /// <c>repocontext_bootstrap</c> surface, where the path is host configuration
    /// rather than caller input.
    /// </para>
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRepoContextTools(
        this IServiceCollection services,
        bool enableWrites = false,
        bool workspaceMode = false,
        string? workspaceRoot = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the workspace guard. A supplied root produces a fail-closed
        // guard that rejects any add-repo path escaping it; an absent root produces
        // a disabled guard that permits any path (the single-repository default).
        // TryAdd means a host (or test harness) that registered its own guard
        // first wins, so the boundary can always be overridden for testing.
        var roots = string.IsNullOrWhiteSpace(workspaceRoot)
            ? Array.Empty<string>()
            : new[] { workspaceRoot };

        // Advertisement must match enforcement: workspace mode without a root
        // cannot honour repocontext_add_repo's promised boundary, so the tool is
        // not offered at all. The handler re-checks the *effective* DI-resolved
        // guard at invocation, which is what covers a host that registered its own
        // non-enforcing guard here despite passing a root.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup>(
                new RepoContextToolGroup(enableWrites, workspaceMode, workspaceGuarded: roots.Length != 0)));

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
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<Orleans.Serialization.Serializer>(),
                sp.GetRequiredService<ILogger<EmbeddingRepoContextVectorIngestor>>(),
                sp.GetService<IEmbeddingProvider>()));
        services.TryAddSingleton<RepoContextVectorCache>();
        services.TryAddSingleton<RepoContextVectorPlaneReDeriver>();
        services.TryAddSingleton<RepoContextVectorWriter>();
        services.TryAddSingleton<RepoContextEmbeddingGapScanner>();

        // The retrieval plane. The approximate index is the default: it is persisted,
        // so a restart reloads it instead of re-scanning every stored vector, and its
        // query cost is sub-linear in the corpus rather than proportional to it. The
        // brute-force exact scan stays registered either way - it answers while an
        // index is still building, and it remains the correctness oracle the recall
        // measurements are taken against. A host selects between them with
        // RepoContextIndexingOptions.SemanticRetrieval; whichever answers, the
        // response says which guarantee it carries through its retrieval path.
        services.TryAddSingleton<ExactKnnSemanticIndex>();
        services.TryAddSingleton<RepoContextAnnOptions>();
        services.TryAddSingleton<IRepoContextAnnBackingFactory, LatticeRepoContextAnnBackingFactory>();
        services.TryAddSingleton<RepoContextAnnIndexRegistry>();
        services.TryAddSingleton<IRepoContextAnnIndex>(
            sp => sp.GetRequiredService<RepoContextAnnIndexRegistry>());

        // The build scheduler and its startup sweep. The index build is what makes
        // queries fast, so arming it from a query made the acceleration reachable
        // only from the thing it accelerates: nothing resumed it after a process
        // death, and an idle repository never built at all. The sweep arms a durable
        // reminder-anchored coordinator per (repository, embedding space) with no
        // traffic whatsoever, and the coordinator survives a restart. TryAdd means a
        // host or test harness can substitute the scheduler.
        services.TryAddSingleton(sp => new RepoContextAnnIndexScheduler(
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<RepoContextIndexingOptions>(),
            sp.GetRequiredService<ILogger<RepoContextAnnIndexScheduler>>(),
            sp.GetService<IEmbeddingProvider>()));
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, RepoContextAnnIndexSweepService>());

        services.TryAddSingleton<IRepoContextSemanticIndex>(sp =>
        {
            var exact = sp.GetRequiredService<ExactKnnSemanticIndex>();
            return sp.GetRequiredService<RepoContextIndexingOptions>().SemanticRetrieval
                == RepoContextSemanticRetrievalMode.Exact
                ? exact
                : new AnnRepoContextSemanticIndex(
                    sp.GetRequiredService<IRepoContextAnnIndex>(),
                    exact,
                    sp.GetRequiredService<ILogger<AnnRepoContextSemanticIndex>>());
        });

        // The shared vector-plane readiness signal. It is fed at the single seam every
        // query funnels through (the search service, once per call) and read by the
        // host's readiness probe, so a box that cannot serve semantic retrieval stops
        // reporting itself fully ready. TryAdd means a host or test harness can
        // substitute one with a different fault hold-down.
        services.TryAddSingleton(sp =>
            new RepoContextRetrievalReadinessState(sp.GetRequiredService<TimeProvider>()));
        services.TryAddSingleton<RepoContextSearchService>(sp =>
            new RepoContextSearchService(
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<Orleans.Serialization.Serializer>(),
                sp.GetRequiredService<IRepoContextSemanticIndex>(),
                sp.GetRequiredService<RepoContextStore>(),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<ILogger<RepoContextSearchService>>(),
                sp.GetService<IEmbeddingProvider>(),
                sp.GetRequiredService<RepoContextRetrievalReadinessState>()));

        // The warmup driver behind the host's vector-plane readiness probe: it drives a
        // real semantic query so readiness reports demonstrated capability instead of
        // waiting for traffic an orchestrator will not route to a not-ready box. TryAdd
        // means a host or test harness can substitute it.
        services.TryAddSingleton<IRepoContextRetrievalWarmup, RepoContextRetrievalWarmup>();

        services.TryAddSingleton<RepoContextBootstrapService>();
        services.TryAddSingleton(TimeProvider.System);

        // The read-only structural-graph adapter behind the outline / changed / related
        // tools. It is a pure projection over the file, symbol, content, and reverse
        // cross-reference records the reconcilers maintain, and reaches the workspace
        // (for changed) only through the fail-closed RepoContextWorkspaceGuard. TryAdd
        // means a host or test harness can substitute it.
        services.TryAddSingleton<RepoContextGraphService>();

        // The per-session reuse-bookkeeping store behind repocontext_context's reuse
        // economics. It persists, per (repoId, sessionId), exactly what a prior bundle
        // call delivered so a later call never re-charges for it. TryAdd means a host or
        // test harness can substitute it.
        services.TryAddSingleton<RepoContextSessionStore>();

        // The usage-accounting recorder behind repocontext_stats. It records, per answered
        // context call, the exact response tokens spent and a conservative estimate of the
        // whole-file reads replaced, keeps a bounded in-memory window for the read-only stats
        // tool, and emits the same figures as telemetry counters. TryAdd means a host or test
        // harness can substitute it (for example with an in-memory capturing double).
        services.TryAddSingleton<IRepoContextUsageRecorder>(
            sp => new RepoContextUsageRecorder(sp.GetRequiredService<TimeProvider>()));

        // The read-only budgeted context-bundle adapter behind repocontext_context. It
        // composes the search and graph services with the shared token counter to pack a
        // ranked, explained bundle under a hard token ceiling. TryAdd means a host or
        // test harness can substitute it.
        services.TryAddSingleton<RepoContextBundleService>();

        // The symbol-structural reconcile seam: a language-dispatching extractor
        // (only C#/Roslyn is registered today; other languages fall through to no
        // output) and the reconciler that upserts and prunes per-symbol records as
        // files change. TryAdd means a host or test harness can substitute either.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILanguageSymbolExtractor, CSharpSymbolExtractor>());
        services.TryAddSingleton<ISymbolExtractor, SymbolExtractorDispatcher>();
        services.TryAddSingleton<RepoContextSymbolReconciler>();

        // The content-projection reconcile seam: projects each text file's bounded
        // body text into the dedicated content tree during the structural reconcile
        // (decoupled from embeddings) so the keyword/degraded search path can rank
        // over file content. TryAdd means a host or test harness can substitute it.
        services.TryAddSingleton<RepoContextContentReconciler>();
        services.TryAddSingleton(RepoContextIndexingOptions.FromEnvironment());

        // The stable replica identity authored onto every agent-memory CRDT write.
        // The base default is the single-cluster local id; the replication companion
        // (EnableRepoContextMultiCluster) registers a cluster-id identity so
        // cross-cluster concurrent memory writes mint distinct dots and both survive
        // the merge. TryAdd means the companion's registration wins when present.
        services.TryAddSingleton<IRepoContextReplicaIdentity, LocalRepoContextReplicaIdentity>();
        services.TryAddSingleton<RepoContextStore>();

        // The shared BPE token counter: constructs its tiktoken tokenizer once from the
        // configured tokenizer profile and is reused by the reconcile path (per-file
        // token counts) and the retrieval surface (token budgets). TryAdd means a host
        // or test harness that registers its own counter first wins.
        services.TryAddSingleton<IRepoContextTokenCounter>(sp =>
            new TiktokenRepoContextTokenCounter(sp.GetRequiredService<RepoContextIndexingOptions>()));

        // The background indexing runner runs each onboarding pass off the request
        // thread, bound to the host lifetime, so a client disconnect never aborts an
        // index. The reminder-anchored job grain is auto-discovered by Orleans from
        // this library assembly; it requires the host to have configured a reminder
        // service (the container host does).
        services.TryAddSingleton<IRepoIndexRunner, RepoIndexRunner>();

        // Resolve the credential every background indexing run assumes. The default
        // resolves none, so a run carries whatever ambient credential the enqueue
        // captured - correct for an in-process host with no access gate. A host that
        // enforces a fail-closed gate (the container) registers its own authority
        // BEFORE this call so its fixed local-agent credential wins, ensuring a
        // reminder-driven resume writes under the same subject as the original pass.
        services.TryAddSingleton<IRepoIndexRunAuthority, NullRepoIndexRunAuthority>();

        // Bind the per-repository TTL policy under the named-options convention
        // (IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)) and validate every
        // instance at first resolve. The memory-writing tools consume these.
        services.AddOptions<RepoContextTtlOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<RepoContextTtlOptions>, RepoContextTtlOptionsValidator>());

        return services;
    }
}
