namespace MultiSiteManufacturing.Host;

/// <summary>
/// Identifies the current silo instance — used by services that
/// partition behaviour by silo (for example the M12c silo-partition
/// chaos preset, which routes writes to disjoint sub-sets per silo so
/// two browser tabs pointed at <c>localhost:5001</c> and
/// <c>localhost:5002</c> can simulate writing on either side of a
/// network partition).
/// </summary>
/// <param name="Id">
/// Short silo identifier supplied via the <c>--silo-id</c> command-line
/// argument (<c>"a"</c> or <c>"b"</c>). The seeded default is <c>"a"</c>.
/// </param>
/// <param name="IsPrimary">
/// <c>true</c> when this silo is the primary (silo A). Only the primary
/// runs the <see cref="MultiSiteManufacturing.Host.Inventory.InventorySeeder"/>
/// to prevent secondary-silo restarts from re-seeding the shared
/// storage account.
/// </param>
public sealed record SiloIdentity(string Id, bool IsPrimary);
