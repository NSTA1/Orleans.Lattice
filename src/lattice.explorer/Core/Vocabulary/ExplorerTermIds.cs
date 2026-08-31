namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// The stable identifiers of every term <see cref="ExplorerGlossary"/> defines.
/// </summary>
/// <remarks>
/// <para>
/// A consumer references a constant here rather than spelling a magic string, so
/// a rename is a compile error rather than a silently missing explanation. The
/// values are also usable as element-id prefixes for the help disclosure that
/// renders a term, so they are lower-case and hyphenated.
/// </para>
/// <para>
/// Adding a term means adding a constant here and the matching entry to
/// <see cref="ExplorerGlossary"/>; a guard test fails if the two ever disagree.
/// </para>
/// </remarks>
public static class ExplorerTermIds
{
    /// <summary>The catalog's tree kind.</summary>
    public const string Trees = "trees";

    /// <summary>The catalog's view kind.</summary>
    public const string Views = "views";

    /// <summary>The catalog's tag-index kind.</summary>
    public const string TagIndexes = "tag-indexes";

    /// <summary>A tree that is live and serving.</summary>
    public const string LifecycleActive = "lifecycle-active";

    /// <summary>A tree deleted but still inside its retention window.</summary>
    public const string LifecycleSoftDeleted = "lifecycle-soft-deleted";

    /// <summary>A tree whose retention window has elapsed and is being removed.</summary>
    public const string LifecyclePurging = "lifecycle-purging";

    /// <summary>The per-tree shard-count badge.</summary>
    public const string ShardCount = "shard-count";

    /// <summary>The per-tree dead-letter-count badge.</summary>
    public const string DeadLetterCount = "dead-letter-count";

    /// <summary>A grouped-reduce view.</summary>
    public const string AggregationView = "aggregation-view";

    /// <summary>A change-history view.</summary>
    public const string HistoryView = "history-view";

    /// <summary>The tree a view projects from.</summary>
    public const string SourceTree = "source-tree";

    /// <summary>The host-registered code that computes a runtime view.</summary>
    public const string ProjectionProvider = "projection-provider";

    /// <summary>The version of the projection logic a view's rows were built with.</summary>
    public const string ProjectionVersion = "projection-version";

    /// <summary>One self-balancing sub-tree of a tree's keyspace.</summary>
    public const string Shard = "shard";

    /// <summary>The node that holds the entries themselves.</summary>
    public const string Leaf = "leaf";

    /// <summary>The write-ahead log.</summary>
    public const string Wal = "wal";

    /// <summary>A conflict-free replicated data type.</summary>
    public const string Crdt = "crdt";

    /// <summary>A write set aside for review rather than discarded.</summary>
    public const string DeadLetter = "dead-letter";

    /// <summary>The background pass that reclaims space from tombstones.</summary>
    public const string Compaction = "compaction";

    /// <summary>Changing a tree's shard count online.</summary>
    public const string Reshard = "reshard";

    /// <summary>Strict schema enforcement.</summary>
    public const string StrictSchema = "strict-schema";

    /// <summary>An isolated slice of the cluster.</summary>
    public const string Tenant = "tenant";

    /// <summary>The tenant the Explorer is currently scoped to.</summary>
    public const string ActiveTenant = "active-tenant";

    /// <summary>The cross-tenant listing scope.</summary>
    public const string AllTenants = "all-tenants";

    /// <summary>A tenant's consumption ceiling.</summary>
    public const string Quota = "quota";

    /// <summary>The regions a tenant's data may live in.</summary>
    public const string Residency = "residency";

    /// <summary>One geographic deployment of the cluster.</summary>
    public const string Region = "region";

    /// <summary>A permission attached to an identity.</summary>
    public const string Grant = "grant";

    /// <summary>An identity with administration rights over a tenant.</summary>
    public const string AdminSubject = "admin-subject";

    /// <summary>The operator-facing tenant administration area.</summary>
    public const string TenantAdministrationArea = "tenant-administration-area";

    /// <summary>The self-service area for the signed-in identity's own tenant.</summary>
    public const string MyTenantArea = "my-tenant-area";

    /// <summary>A surface the cluster only serves to a signed-in identity.</summary>
    public const string SignInRequired = "sign-in-required";

    /// <summary>A surface whose feature is not enabled on this cluster.</summary>
    public const string NotAvailableHere = "not-available-here";
}
