namespace Orleans.Lattice.Api.State;

/// <summary>
/// Centralised Orleans serialization alias constants for every
/// <c>Orleans.Lattice.Api.State</c> type that participates in the wire
/// format. Each alias is a short, fixed string that gives a type a stable
/// wire identity independent of its CLR name. State-API aliases use the
/// <c>ola.</c> prefix (Orleans Lattice Api) to avoid collision with the
/// core (<c>ol.</c>) and replication (<c>olr.</c>) alias namespaces.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire and
/// persisted format. New types append new constants.
/// </remarks>
public static class ApiStateTypeAliases
{
    /// <summary>Alias for <see cref="TreeConfigSummary"/>.</summary>
    public const string TreeConfigSummary = "ola.tc";

    /// <summary>Alias for <see cref="TreeStateSummary"/>.</summary>
    public const string TreeStateSummary = "ola.ts";

    /// <summary>Alias for <see cref="ShardStateSummary"/>.</summary>
    public const string ShardStateSummary = "ola.ss";

    /// <summary>Alias for <see cref="NodeStateSummary"/>.</summary>
    public const string NodeStateSummary = "ola.ns";

    /// <summary>Alias for <see cref="ViewStateSummary"/>.</summary>
    public const string ViewStateSummary = "ola.vs";

    /// <summary>Alias for <see cref="EntryRecord"/>.</summary>
    public const string EntryRecord = "ola.er";

    /// <summary>Alias for <see cref="CatalogRequest"/>.</summary>
    public const string CatalogRequest = "ola.cr";

    /// <summary>Alias for <see cref="TreeCatalogEntry"/>.</summary>
    public const string TreeCatalogEntry = "ola.tce";

    /// <summary>Alias for <see cref="TreeCatalogPage"/>.</summary>
    public const string TreeCatalogPage = "ola.tcp";

    /// <summary>Alias for <see cref="ViewCatalogPage"/>.</summary>
    public const string ViewCatalogPage = "ola.vcp";

    /// <summary>Alias for <see cref="StructureRequest"/>.</summary>
    public const string StructureRequest = "ola.sr";

    /// <summary>Alias for <see cref="EntryScanRequest"/>.</summary>
    public const string EntryScanRequest = "ola.esr";

    /// <summary>Alias for <see cref="EntryScanMode"/>.</summary>
    public const string EntryScanMode = "ola.esm";

    /// <summary>Alias for <see cref="EntryScanCancelRequest"/>.</summary>
    public const string EntryScanCancelRequest = "ola.escr";

    /// <summary>Alias for <see cref="StateChangeNotification"/>.</summary>
    public const string StateChangeNotification = "ola.scn";

    /// <summary>Alias for <see cref="StateObserveRequest"/>.</summary>
    public const string StateObserveRequest = "ola.sor";

    /// <summary>Alias for <see cref="TreeMetricsRequest"/>.</summary>
    public const string TreeMetricsRequest = "ola.tmr";

    /// <summary>Alias for <see cref="ShardHotness"/>.</summary>
    public const string ShardHotness = "ola.sh";

    /// <summary>Alias for <see cref="TreeMetrics"/>.</summary>
    public const string TreeMetrics = "ola.tm";

    /// <summary>Alias for <see cref="TreeMetricsSnapshot"/>.</summary>
    public const string TreeMetricsSnapshot = "ola.tms";

    /// <summary>Alias for <see cref="ClusterInfoRequest"/>.</summary>
    public const string ClusterInfoRequest = "ola.cir";

    /// <summary>Alias for <see cref="ClusterInfo"/>.</summary>
    public const string ClusterInfo = "ola.ci";

    /// <summary>Alias for <see cref="TagIndexStateSummary"/>.</summary>
    public const string TagIndexStateSummary = "ola.tis";

    /// <summary>Alias for <see cref="TagIndexCatalogPage"/>.</summary>
    public const string TagIndexCatalogPage = "ola.tip";

    /// <summary>Alias for <see cref="TagValueCatalogPage"/>.</summary>
    public const string TagValueCatalogPage = "ola.tvp";

    /// <summary>Alias for <see cref="EntryHistoryRequest"/>.</summary>
    public const string EntryHistoryRequest = "ola.ehq";

    /// <summary>Alias for <see cref="EntryRevisionRecord"/>.</summary>
    public const string EntryRevisionRecord = "ola.erev";

    /// <summary>Alias for <see cref="RevisionRetention"/>.</summary>
    public const string RevisionRetention = "ola.rret";

    /// <summary>Alias for <see cref="EntryHistoryBound"/>.</summary>
    public const string EntryHistoryBound = "ola.ehb";

    /// <summary>Alias for <see cref="CoveredTreeCatalogPage"/>.</summary>
    public const string CoveredTreeCatalogPage = "ola.ctp";

    /// <summary>Alias for <see cref="TagMemberScanRequest"/>.</summary>
    public const string TagMemberScanRequest = "ola.tmsr";

    /// <summary>Alias for <see cref="TagMember"/>.</summary>
    public const string TagMember = "ola.tmem";

    /// <summary>Alias for <see cref="TagMemberScanPage"/>.</summary>
    public const string TagMemberScanPage = "ola.tmsp";
}
