using System.Collections.Frozen;

namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// The Explorer's single glossary: one definition per term, consumed by every
/// surface so the same concept is described identically wherever it appears.
/// </summary>
/// <remarks>
/// <para>
/// A surface never writes its own prose for a term. It looks the term up here
/// and hands the result to the help disclosure:
/// </para>
/// <code>
/// var term = ExplorerGlossary.Get(ExplorerTermIds.Shard);
/// // &lt;LatticeHelp Id="@term.Id" Term="@term.Label" Explanation="@term.Explanation" /&gt;
/// // and on the control it explains:
/// // aria-describedby="@LatticeHelp.ExplanationElementId(term.Id)"
/// </code>
/// <para>
/// The table is a <see cref="FrozenDictionary{TKey, TValue}"/> built once in the
/// static initialiser. A lookup is a single ordinal hash probe and allocates
/// nothing, which matters because a lookup happens per rendered badge.
/// </para>
/// </remarks>
public static class ExplorerGlossary
{
    private static readonly ExplorerTerm[] AllTerms =
    [
        new()
        {
            Id = ExplorerTermIds.Trees,
            Label = "Trees",
            Explanation = "A tree is one keyspace of string keys and byte-array values, split across the cluster so it can grow past a single machine.",
            DocsLink = ExplorerDocsLinks.TreeRegistry,
        },
        new()
        {
            Id = ExplorerTermIds.Views,
            Label = "Views",
            Explanation = "A view is a projection kept up to date from a source tree, so a derived shape can be read directly instead of being recomputed on every query.",
            DocsLink = ExplorerDocsLinks.MaterialisedViews,
        },
        new()
        {
            Id = ExplorerTermIds.TagIndexes,
            Label = "Tag indexes",
            Explanation = "A tag index lists the keys of a tree that carry a given tag, so entries can be found by tag rather than only by key.",
            DocsLink = ExplorerDocsLinks.Api,
        },
        new()
        {
            Id = ExplorerTermIds.LifecycleActive,
            Label = "Active",
            Explanation = "The tree is live and accepting reads and writes.",
            DocsLink = ExplorerDocsLinks.TreeRegistry,
        },
        new()
        {
            Id = ExplorerTermIds.LifecycleSoftDeleted,
            Label = "Soft-deleted",
            Explanation = "The tree has been deleted but is still inside its retention window, so its data stays readable until it is purged.",
            DocsLink = ExplorerDocsLinks.TreeDeletion,
        },
        new()
        {
            Id = ExplorerTermIds.LifecyclePurging,
            Label = "Purging",
            Explanation = "The tree's retention window has elapsed and its data is being physically removed. It cannot be recovered once this finishes.",
            DocsLink = ExplorerDocsLinks.TreeDeletion,
        },
        new()
        {
            Id = ExplorerTermIds.ShardCount,
            Label = "Shards",
            Explanation = "How many shards this tree's keyspace is split across. Each shard is an independent sub-tree, so more shards means more write parallelism.",
            DocsLink = ExplorerDocsLinks.TreeSizing,
        },
        new()
        {
            Id = ExplorerTermIds.DeadLetterCount,
            Label = "Dead-lettered writes",
            Explanation = "Writes this tree rejected and set aside for review rather than discarding. Under strict schema enforcement a write that fails validation lands here.",
            DocsLink = ExplorerDocsLinks.DeadLetterQueue,
        },
        new()
        {
            Id = ExplorerTermIds.AggregationView,
            Label = "Aggregation view",
            Explanation = "A view that groups its source tree's entries and reduces each group to one row, so a total or a count is read directly instead of being recomputed.",
            DocsLink = ExplorerDocsLinks.MaterialisedViews,
        },
        new()
        {
            Id = ExplorerTermIds.HistoryView,
            Label = "Change-history view",
            Explanation = "A view that accumulates the changes made to its source tree over time. It backs the History surface rather than holding directly inspectable values.",
            DocsLink = ExplorerDocsLinks.HistoryViews,
        },
        new()
        {
            Id = ExplorerTermIds.SourceTree,
            Label = "Source tree",
            Explanation = "The tree this view projects from. Every change to the source tree flows into the view.",
            DocsLink = ExplorerDocsLinks.MaterialisedViews,
        },
        new()
        {
            Id = ExplorerTermIds.ProjectionProvider,
            Label = "Projection provider",
            Explanation = "The host-registered code that computes this view's rows. Shown for views registered while the cluster is running rather than at startup.",
            DocsLink = ExplorerDocsLinks.ProjectionRebuild,
        },
        new()
        {
            Id = ExplorerTermIds.ProjectionVersion,
            Label = "Projection version",
            Explanation = "The version of the projection logic this view's stored rows were built with. Raising it rebuilds the view from its source tree.",
            DocsLink = ExplorerDocsLinks.ProjectionRebuild,
        },
        new()
        {
            Id = ExplorerTermIds.Shard,
            Label = "Shard",
            Explanation = "One self-balancing sub-tree of a tree's keyspace. Shards are the unit of distribution, so a tree scales by having more of them.",
            DocsLink = ExplorerDocsLinks.TreeStructure,
        },
        new()
        {
            Id = ExplorerTermIds.Leaf,
            Label = "Leaf",
            Explanation = "The node at the bottom of a shard that holds the entries themselves. The levels above it only route a key down to the right leaf.",
            DocsLink = ExplorerDocsLinks.TreeStructure,
        },
        new()
        {
            Id = ExplorerTermIds.Wal,
            Label = "Write-ahead log (WAL)",
            Explanation = "The durable log every write is appended to before it is applied, so a write survives a restart and can be replayed to another replica.",
            DocsLink = ExplorerDocsLinks.Wal,
        },
        new()
        {
            Id = ExplorerTermIds.Crdt,
            Label = "CRDT",
            Explanation = "A conflict-free replicated data type: a value whose concurrent updates merge by a fixed rule, so replicas agree without locks or consensus.",
            DocsLink = ExplorerDocsLinks.Crdt,
        },
        new()
        {
            Id = ExplorerTermIds.DeadLetter,
            Label = "Dead letter",
            Explanation = "A write that was rejected and set aside for review instead of being discarded, so it can be inspected, corrected and replayed.",
            DocsLink = ExplorerDocsLinks.DeadLetterQueue,
        },
        new()
        {
            Id = ExplorerTermIds.Compaction,
            Label = "Compaction",
            Explanation = "The background pass that reclaims space by removing tombstones - the markers left by deleted entries - once they can no longer affect a merge.",
            DocsLink = ExplorerDocsLinks.Compaction,
        },
        new()
        {
            Id = ExplorerTermIds.Reshard,
            Label = "Reshard",
            Explanation = "Changing how many shards a tree's keyspace is split across while it keeps serving reads and writes.",
            DocsLink = ExplorerDocsLinks.OnlineReshard,
        },
        new()
        {
            Id = ExplorerTermIds.StrictSchema,
            Label = "Strict schema enforcement",
            Explanation = "The mode in which a write that does not match the tree's registered schema is rejected and dead-lettered rather than stored.",
            DocsLink = ExplorerDocsLinks.SchemaEnforcement,
        },
        new()
        {
            Id = ExplorerTermIds.Tenant,
            Label = "Tenant",
            Explanation = "An isolated slice of the cluster with its own data, quota and residency. Every tree belongs to exactly one tenant.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.DefaultTenant,
            Label = "Default tenant",
            Explanation = "The reserved tenant that owns every tree with no tenant prefix. It always exists and cannot be suspended or deleted, so a cluster that was never made multi-tenant has this one and nothing else.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.ActiveTenant,
            Label = "Active tenant",
            Explanation = "The tenant the Explorer is currently reading as. Everything listed belongs to it, so changing the active tenant changes what you see.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.AllTenants,
            Label = "All tenants",
            Explanation = "Lists items across every tenant you can reach instead of only the active one. It needs a cluster-wide grant, so it is not always offered.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.Quota,
            Label = "Quota",
            Explanation = "The ceiling on what a tenant may consume - entries, bytes and request rate. A write that would exceed it is refused.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.Residency,
            Label = "Residency",
            Explanation = "The set of regions a tenant's data is allowed to live in. A write in a region outside that set is refused.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.Region,
            Label = "Region",
            Explanation = "One deployment of the cluster in a geographic location. Regions replicate to each other, and residency decides which may hold a tenant's data.",
            DocsLink = ExplorerDocsLinks.Tenancy,
        },
        new()
        {
            Id = ExplorerTermIds.Grant,
            Label = "Grant",
            Explanation = "A permission attached to your identity that allows one kind of operation. Without it the cluster refuses the call, so the Explorer cannot show the result.",
            DocsLink = ExplorerDocsLinks.ManagingAccess,
        },
        new()
        {
            Id = ExplorerTermIds.AdminSubject,
            Label = "Administrator",
            Explanation = "An identity granted administration rights over a tenant, so it can change that tenant's quota, residency and grants.",
            DocsLink = ExplorerDocsLinks.ManagingAccess,
        },
        new()
        {
            Id = ExplorerTermIds.TenantAdministrationArea,
            Label = ExplorerVocabulary.TenantAdministrationArea,
            Explanation = "The operator surface: every tenant in the cluster, with its quota, residency and administrators. It needs a tenant-administration grant.",
            DocsLink = ExplorerDocsLinks.Explorer,
        },
        new()
        {
            Id = ExplorerTermIds.MyTenantArea,
            Label = ExplorerVocabulary.MyTenantArea,
            Explanation = "The self-service surface: the settings and usage of the one tenant you are signed in to, with no administration rights over any other.",
            DocsLink = ExplorerDocsLinks.Explorer,
        },
        new()
        {
            Id = ExplorerTermIds.SignInRequired,
            Label = "Sign-in required",
            Explanation = "This surface reads data the cluster serves only to a signed-in identity, so it stays available but empty until you sign in.",
            DocsLink = ExplorerDocsLinks.SigningIn,
        },
        new()
        {
            Id = ExplorerTermIds.NotAvailableHere,
            Label = "Not available on this cluster",
            Explanation = "The feature behind this surface is not enabled on the cluster you are connected to, so there is nothing for it to show.",
            DocsLink = ExplorerDocsLinks.RunningTheExplorer,
        },
    ];

    private static readonly FrozenDictionary<string, ExplorerTerm> ById =
        AllTerms.ToFrozenDictionary(term => term.Id, StringComparer.Ordinal);

    /// <summary>
    /// Every term the glossary defines, in declaration order. The order is
    /// stable but carries no meaning; consumers look terms up by id.
    /// </summary>
    public static IReadOnlyList<ExplorerTerm> Terms => AllTerms;

    /// <summary>The number of terms the glossary defines.</summary>
    public static int Count => AllTerms.Length;

    /// <summary>
    /// Looks a term up by id without throwing, for a call site that has an id it
    /// cannot vouch for.
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <param name="term">The term when one is defined, otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the glossary defines <paramref name="id"/>.</returns>
    public static bool TryGet(string? id, out ExplorerTerm? term)
    {
        if (string.IsNullOrEmpty(id))
        {
            term = null;
            return false;
        }

        return ById.TryGetValue(id, out term);
    }

    /// <summary>
    /// The term with the given id, or <see langword="null"/> when the glossary
    /// does not define it (including for a null or empty id).
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns>The term, or <see langword="null"/>.</returns>
    public static ExplorerTerm? Find(string? id) => TryGet(id, out var term) ? term : null;

    /// <summary>
    /// The term with the given id. Use this where the id is a constant from
    /// <see cref="ExplorerTermIds"/> and a miss is a programming error.
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns>The term.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    /// <exception cref="KeyNotFoundException">The glossary defines no such term.</exception>
    public static ExplorerTerm Get(string id)
    {
        ArgumentNullException.ThrowIfNull(id);

        return ById.TryGetValue(id, out var term)
            ? term
            : throw new KeyNotFoundException("The Explorer glossary defines no term with id '" + id + "'.");
    }

    /// <summary>Whether the glossary defines a term with the given id.</summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns><see langword="true"/> when the term is defined.</returns>
    public static bool Contains(string? id) => TryGet(id, out _);

    /// <summary>
    /// The one-line explanation for a term, or <see langword="null"/> when the
    /// glossary does not define it. Binding this straight onto the help
    /// disclosure renders nothing for an unknown term rather than an empty
    /// affordance.
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns>The explanation, or <see langword="null"/>.</returns>
    public static string? ExplanationFor(string? id) => Find(id)?.Explanation;

    /// <summary>
    /// The short label for a term, or <see langword="null"/> when the glossary
    /// does not define it.
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns>The label, or <see langword="null"/>.</returns>
    public static string? LabelFor(string? id) => Find(id)?.Label;

    /// <summary>
    /// Where to read more about a term, or <see langword="null"/> when the term
    /// is unknown or has no document.
    /// </summary>
    /// <param name="id">A term id from <see cref="ExplorerTermIds"/>.</param>
    /// <returns>The repository-relative documentation path, or <see langword="null"/>.</returns>
    public static string? DocsLinkFor(string? id) => Find(id)?.DocsLink;

    /// <summary>
    /// The term describing a tree's lifecycle state as the cluster reports it
    /// (<c>Active</c>, <c>SoftDeleted</c>, <c>Purging</c>), matched without
    /// regard to case so a caller can pass the raw value through.
    /// </summary>
    /// <param name="lifecycle">The lifecycle value, or <see langword="null"/>.</param>
    /// <returns>The term, or <see langword="null"/> for an absent or unrecognised value.</returns>
    public static ExplorerTerm? ForLifecycle(string? lifecycle)
    {
        if (string.IsNullOrEmpty(lifecycle))
        {
            return null;
        }

        // Compared against the enum spellings the state API emits rather than
        // lower-cased into an allocation: the set is three long and closed.
        if (string.Equals(lifecycle, "Active", StringComparison.OrdinalIgnoreCase))
        {
            return Get(ExplorerTermIds.LifecycleActive);
        }

        if (string.Equals(lifecycle, "SoftDeleted", StringComparison.OrdinalIgnoreCase))
        {
            return Get(ExplorerTermIds.LifecycleSoftDeleted);
        }

        return string.Equals(lifecycle, "Purging", StringComparison.OrdinalIgnoreCase)
            ? Get(ExplorerTermIds.LifecyclePurging)
            : null;
    }
}
