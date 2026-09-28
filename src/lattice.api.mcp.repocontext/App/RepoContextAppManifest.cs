using Orleans.Lattice.Apps;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context installable-app manifest: its slug, the embedded resource that
/// carries the JSON manifest, the configuration-independent tool surface it declares, and
/// the app-scoped backup plan computed from its tree declarations.
/// <para>
/// <b>Slug.</b> The slug is <c>repo-context</c>, not <c>repocontext</c>. App tools are
/// advertised as <c>{slug}_{tool}</c>, so with the unhyphenated slug the app-local
/// <c>search</c> tool would surface as <c>repocontext_search</c> - the exact name of the
/// existing group tool - and the app path would be dropped as a collision. With
/// <c>repo-context</c> the app tools (<c>repo-context_search</c>, ...) are advertised
/// alongside the unchanged group tools.
/// </para>
/// <para>
/// <b>Trees.</b> Every tree the package owns (<see cref="RepoContextTrees.AllIncludingLocalDerived"/>)
/// is declared under an app-local name that <b>adopts</b> the existing physical tree
/// (<see cref="AppTreeDeclaration.AdoptedTreeId"/>), so installing the app moves no data.
/// Adopted trees are never structurally granted, need operator-approved ceiling exception
/// scopes to activate, and are never soft-deleted by uninstall. A regression test asserts
/// the declarations agree with <see cref="RepoContextTrees"/>.
/// </para>
/// </summary>
internal static class RepoContextAppManifest
{
    /// <summary>The app slug. Hyphenated so the app tool names cannot collide with the group's <c>repocontext_*</c> tools.</summary>
    internal const string Slug = "repo-context";

    /// <summary>The logical name of the embedded JSON manifest resource, pinned in the project file.</summary>
    internal const string ResourceName = "Orleans.Lattice.Api.Mcp.RepoContext.App.repo-context.app.json";

    /// <summary>The prefix every repository-context group tool name carries.</summary>
    internal const string GroupToolPrefix = "repocontext_";

    /// <summary>
    /// The app-local names of the tools the app surface declares, in manifest order. Each is
    /// the group tool name with <see cref="GroupToolPrefix"/> removed.
    /// <para>
    /// The set is exactly the group's <b>always-on</b> read-only tools - the ones the group
    /// contributes under every host configuration. The app tool surface pairs a manifest's
    /// declarations with the provided implementations all-or-nothing, and the manifest is a
    /// fixed resource, so a declared tool the host configuration withholds would fail the
    /// whole app. Worse, contributing a withheld tool on the app path would bypass the
    /// host's opt-ins: the mutating tools exist only when the host enables writes, and the
    /// path-taking tools (<c>repocontext_changed</c>, the onboarding tools) only when an
    /// enforcing workspace guard is configured. Those stay on the group path alone.
    /// </para>
    /// </summary>
    internal static IReadOnlyList<string> AppToolNames { get; } =
    [
        "health",
        "recall",
        "scan",
        "list_topics",
        "search",
        "index_status",
        "neighbors",
        "outline",
        "related",
        "context",
        "stats",
        "claim_status",
    ];

    /// <summary>
    /// Loads and validates the embedded manifest. Reads only the resource; never loads or
    /// runs app code. A missing or invalid resource returns diagnostics rather than throwing.
    /// </summary>
    /// <returns>The manifest result.</returns>
    internal static AppManifestResult Load()
        => AppManifestResources.Load(typeof(RepoContextAppManifest).Assembly, ResourceName);

    /// <summary>
    /// Maps a group tool name to its app-local name, or returns <see langword="null"/> when
    /// the tool is not part of the app surface.
    /// </summary>
    /// <param name="groupToolName">The group tool name, for example <c>repocontext_search</c>.</param>
    /// <returns>The app-local name, for example <c>search</c>, or <see langword="null"/>.</returns>
    internal static string? LocalNameFor(string? groupToolName)
    {
        if (groupToolName is null || !groupToolName.StartsWith(GroupToolPrefix, StringComparison.Ordinal))
        {
            return null;
        }

        var local = groupToolName.AsSpan(GroupToolPrefix.Length);
        foreach (var name in AppToolNames)
        {
            if (local.SequenceEqual(name))
            {
                return name;
            }
        }

        return null;
    }

    /// <summary>
    /// Computes the app-scoped backup plan from <paramref name="manifest"/>'s tree
    /// declarations: it classifies the trees an app-scoped recovery would re-derive
    /// (those declared <see cref="AppTreeDeclaration.Rebuildable"/>) versus restore
    /// from backup bytes (every other tree). No shipped path consumes the plan yet,
    /// so nothing re-derives or restores on the strength of it. Each tree is named by
    /// its effective physical id - the adopted id when set, otherwise
    /// <c>a/{slug}/{name}</c> - composed for <paramref name="tenant"/>.
    /// </summary>
    /// <param name="manifest">The manifest whose trees to classify.</param>
    /// <param name="tenant">The tenant whose install the plan covers.</param>
    /// <returns>The plan, preserving manifest declaration order within each list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is <see langword="null"/>.</exception>
    /// <exception cref="LatticeTenantAccessDeniedException"><paramref name="tenant"/> is the uninitialised value, which fails closed.</exception>
    internal static RepoContextAppBackupPlan GetBackupPlan(AppManifest manifest, TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        var restore = new List<string>(manifest.Trees.Length);
        var rederive = new List<string>();
        foreach (var tree in manifest.Trees)
        {
            var local = tree.AdoptedTreeId
                ?? string.Concat(LatticeConstants.AppTreePrefix, manifest.Identity.Slug.Value, "/", tree.Name);
            var effective = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, local);
            (tree.Rebuildable ? rederive : restore).Add(effective);
        }

        return new RepoContextAppBackupPlan(restore, rederive);
    }
}
