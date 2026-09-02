using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// The subjects the Explorer's surfaces list, declared once so two surfaces
/// listing the same thing describe it the same way.
/// </summary>
public static class ExplorerSubjects
{
    /// <summary>The catalog's trees.</summary>
    public static ExplorerSubject Trees { get; } = new()
    {
        Id = "trees",
        Singular = "tree",
        Plural = "trees",
        CollectionLabel = ExplorerVocabulary.TreesLabel,
        TermId = ExplorerTermIds.Trees,
        DocsLink = ExplorerDocsLinks.TreeRegistry,
    };

    /// <summary>The catalog's views.</summary>
    public static ExplorerSubject Views { get; } = new()
    {
        Id = "views",
        Singular = "view",
        Plural = "views",
        CollectionLabel = ExplorerVocabulary.ViewsLabel,
        TermId = ExplorerTermIds.Views,
        DocsLink = ExplorerDocsLinks.MaterialisedViews,
    };

    /// <summary>The catalog's tag indexes.</summary>
    public static ExplorerSubject TagIndexes { get; } = new()
    {
        Id = "tag-indexes",
        Singular = "tag index",
        Plural = "tag indexes",
        CollectionLabel = ExplorerVocabulary.TagIndexesLabel,
        TermId = ExplorerTermIds.TagIndexes,
        DocsLink = ExplorerDocsLinks.Api,
    };

    /// <summary>The tenants an operator administers.</summary>
    public static ExplorerSubject Tenants { get; } = new()
    {
        Id = "tenants",
        Singular = "tenant",
        Plural = "tenants",
        CollectionLabel = ExplorerVocabulary.TenantAdministrationArea,
        TermId = ExplorerTermIds.Tenant,
        DocsLink = ExplorerDocsLinks.Tenancy,
    };

    /// <summary>The backups of a tree.</summary>
    public static ExplorerSubject Backups { get; } = new()
    {
        Id = "backups",
        Singular = "backup",
        Plural = "backups",
        CollectionLabel = ExplorerVocabulary.BackupsArea,
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>The grants attached to identities.</summary>
    public static ExplorerSubject Grants { get; } = new()
    {
        Id = "grants",
        Singular = "grant",
        Plural = "grants",
        CollectionLabel = ExplorerVocabulary.AccessArea,
        TermId = ExplorerTermIds.Grant,
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>The writes a tree rejected and set aside.</summary>
    public static ExplorerSubject DeadLetters { get; } = new()
    {
        Id = "dead-letters",
        Singular = "dead-lettered write",
        Plural = "dead-lettered writes",
        CollectionLabel = "Dead letters",
        TermId = ExplorerTermIds.DeadLetter,
        DocsLink = ExplorerDocsLinks.DeadLetterQueue,
    };

    /// <summary>The key and value entries stored in a tree.</summary>
    public static ExplorerSubject Entries { get; } = new()
    {
        Id = "entries",
        Singular = "entry",
        Plural = "entries",
        CollectionLabel = "Data",
        DocsLink = ExplorerDocsLinks.Api,
    };

    /// <summary>The recorded changes to a tree.</summary>
    public static ExplorerSubject Changes { get; } = new()
    {
        Id = "changes",
        Singular = "change",
        Plural = "changes",
        CollectionLabel = "History",
        TermId = ExplorerTermIds.HistoryView,
        DocsLink = ExplorerDocsLinks.HistoryViews,
    };

    /// <summary>The schema versions registered for a tree.</summary>
    public static ExplorerSubject SchemaVersions { get; } = new()
    {
        Id = "schema-versions",
        Singular = "schema version",
        Plural = "schema versions",
        CollectionLabel = "Schema",
        TermId = ExplorerTermIds.StrictSchema,
        DocsLink = ExplorerDocsLinks.ManagingSchema,
    };

    /// <summary>The telemetry signals a cluster publishes.</summary>
    public static ExplorerSubject TelemetrySignals { get; } = new()
    {
        Id = "telemetry-signals",
        Singular = "signal",
        Plural = "signals",
        CollectionLabel = ExplorerVocabulary.TelemetryArea,
        DocsLink = ExplorerDocsLinks.Telemetry,
    };

    /// <summary>The shards a tree's keyspace is split across.</summary>
    public static ExplorerSubject Shards { get; } = new()
    {
        Id = "shards",
        Singular = "shard",
        Plural = "shards",
        CollectionLabel = "Topology",
        TermId = ExplorerTermIds.Shard,
        DocsLink = ExplorerDocsLinks.TreeStructure,
    };

    /// <summary>The metrics a tree publishes.</summary>
    public static ExplorerSubject Metrics { get; } = new()
    {
        Id = "metrics",
        Singular = "metric",
        Plural = "metrics",
        CollectionLabel = "Metrics",
        DocsLink = ExplorerDocsLinks.Metrics,
    };

    /// <summary>The per-selection surfaces the detail panel can show.</summary>
    public static ExplorerSubject DetailSurfaces { get; } = new()
    {
        Id = "detail-surfaces",
        Singular = "detail surface",
        Plural = "detail surfaces",
        CollectionLabel = "Detail",
        DocsLink = ExplorerDocsLinks.Explorer,
    };

    /// <summary>
    /// The catalog subject matching a kind of catalog listing, so a panel driven
    /// by <see cref="CatalogKind"/> can pick its copy without a switch of its
    /// own.
    /// </summary>
    /// <param name="kind">The catalog kind.</param>
    /// <returns>
    /// The matching subject; <see cref="Trees"/> for a kind the enumeration does
    /// not name, because the catalog's tree listing is its default.
    /// </returns>
    public static ExplorerSubject ForCatalogKind(CatalogKind kind) => kind switch
    {
        CatalogKind.Views => Views,
        CatalogKind.TagIndexes => TagIndexes,
        _ => Trees,
    };
}
