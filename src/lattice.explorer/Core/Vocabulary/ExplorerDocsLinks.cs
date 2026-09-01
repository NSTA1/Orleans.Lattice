namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// The repository-relative documentation paths a glossary entry points at, so
/// "where to read more" is declared once rather than re-typed at each call site.
/// </summary>
/// <remarks>
/// These are repository-relative paths, not URLs, because the Explorer is hosted
/// in several heads (web, MAUI, embedded) whose documentation base differs. A
/// head resolves a path against whatever base it publishes documentation under.
/// </remarks>
public static class ExplorerDocsLinks
{
    /// <summary>The tree registry: how trees are created, listed and named.</summary>
    public const string TreeRegistry = "docs/lattice/tree-registry.md";

    /// <summary>The shape of a tree: shard roots, internal nodes and leaves.</summary>
    public const string TreeStructure = "docs/lattice/tree-structure.md";

    /// <summary>Choosing a shard count for a tree.</summary>
    public const string TreeSizing = "docs/lattice/tree-sizing.md";

    /// <summary>Soft deletion, the retention window and purging.</summary>
    public const string TreeDeletion = "docs/lattice/tree-deletion.md";

    /// <summary>Materialised views and how they project from a source tree.</summary>
    public const string MaterialisedViews = "docs/lattice/materialised-views.md";

    /// <summary>Change-history views.</summary>
    public const string HistoryViews = "docs/lattice/history-views.md";

    /// <summary>Rebuilding a projection after its provider or version changes.</summary>
    public const string ProjectionRebuild = "docs/lattice/projection-rebuild.md";

    /// <summary>The public API, which is where tag indexes are described.</summary>
    public const string Api = "docs/lattice/api.md";

    /// <summary>The write-ahead log.</summary>
    public const string Wal = "docs/lattice/wal.md";

    /// <summary>The conflict-free replicated data types the store is built on.</summary>
    public const string Crdt = "docs/crdt/readme.md";

    /// <summary>Tombstone compaction.</summary>
    public const string Compaction = "docs/lattice/tombstone-compaction.md";

    /// <summary>Online resharding.</summary>
    public const string OnlineReshard = "docs/lattice/online-reshard.md";

    /// <summary>The dead-letter queue that holds rejected writes.</summary>
    public const string DeadLetterQueue = "docs/lattice.schema/dead-letter-queue.md";

    /// <summary>Strict schema enforcement and what it rejects.</summary>
    public const string SchemaEnforcement = "docs/lattice.schema/schema-enforcement.md";

    /// <summary>Tenancy: isolation, quota, residency and administration.</summary>
    public const string Tenancy = "docs/lattice.tenancy/README.md";

    /// <summary>Managing access grants from the Explorer.</summary>
    public const string ManagingAccess = "docs/lattice.explorer/managing-access.md";

    /// <summary>Managing backups from the Explorer.</summary>
    public const string ManagingBackups = "docs/lattice.explorer/managing-backups.md";

    /// <summary>Managing schema from the Explorer.</summary>
    public const string ManagingSchema = "docs/lattice.explorer/managing-schema.md";

    /// <summary>What the Explorer is and how its areas are organised.</summary>
    public const string Explorer = "docs/lattice.explorer/README.md";

    /// <summary>Writing an Explorer plugin, which is what registers a surface.</summary>
    public const string WritingAPlugin = "docs/lattice.explorer/writing-a-plugin.md";

    /// <summary>Running the Explorer, including which features a cluster enables.</summary>
    public const string RunningTheExplorer = "docs/lattice.explorer/running-the-explorer.md";

    /// <summary>Connecting the Explorer to an authenticating state API.</summary>
    public const string SigningIn = "docs/lattice.explorer/connecting-to-an-auth-enabled-state-api.md";

    /// <summary>The telemetry surface.</summary>
    public const string Telemetry = "docs/lattice.api.telemetry/README.md";

    /// <summary>The metrics a tree publishes.</summary>
    public const string Metrics = "docs/lattice/metrics.md";
}
