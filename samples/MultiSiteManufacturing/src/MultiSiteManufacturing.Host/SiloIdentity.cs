namespace MultiSiteManufacturing.Host;

/// <summary>
/// Identifies the current silo instance — used by services that
/// partition behaviour by silo (for example the silo-partition
/// chaos preset) and by the cross-cluster replication subsystem,
/// which needs the cluster name to stamp replog keys and to select
/// the matching <c>appsettings.cluster.{name}.json</c> overlay.
/// </summary>
/// <param name="Id">
/// Short silo identifier supplied via the <c>--silo-id</c> command-line
/// argument (<c>"a"</c> or <c>"b"</c>).
/// </param>
/// <param name="IsPrimary">
/// <c>true</c> when this silo is the primary (silo A). Only the primary
/// runs the <see cref="MultiSiteManufacturing.Host.Inventory.InventorySeeder"/>
/// to prevent secondary-silo restarts from re-seeding the shared
/// storage account. With cross-cluster replication, seeding is
/// additionally restricted to the primary silo of the
/// <see cref="MultiSiteManufacturing.Host.SiloIdentity.ClusterName"/>
/// == <c>"us"</c> cluster — cluster <c>eu</c> receives its
/// seed via replication.
/// </param>
/// <param name="ClusterName">
/// Short cluster name (<c>"us"</c> or <c>"eu"</c>) supplied
/// via the <c>--cluster</c> command-line argument. Selects the
/// per-cluster Orleans <c>ClusterId</c>, port offsets, connection
/// string, and replication topology.
/// </param>
public sealed record SiloIdentity(string Id, bool IsPrimary, string ClusterName = "us");

