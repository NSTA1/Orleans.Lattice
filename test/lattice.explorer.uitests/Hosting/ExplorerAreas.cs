namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Every native area the Explorer ships, with its primary page and one deeper page,
/// and whether the test world shows it to its administrator.
/// </summary>
/// <remarks>
/// The test world serves every control plane but two: it runs no metrics backend, so
/// Telemetry is hidden, and no tenancy add-on, so Tenancy is hidden. A hidden area is
/// still swept, reflowed and deep-linked: what a caller gets at its address is the
/// not-found page, and that page is held to the same bar.
/// </remarks>
internal static class ExplorerAreas
{
    /// <summary>Every area, in directory order.</summary>
    public static IReadOnlyList<ExplorerArea> All { get; } =
    [
        new("data", "Data", "/data", $"/data/{ExplorerWorld.DemoTree}", ShownToAdmin: true, TenantScoped: true),
        new("apps", "Apps", "/apps", "/apps/catalogue", ShownToAdmin: true, TenantScoped: true),
        new("access", "Access", "/access", "/access/groups", ShownToAdmin: true, TenantScoped: false),
        new("schema", "Schema", "/schema", $"/schema/{ExplorerWorld.DemoTree}", ShownToAdmin: true, TenantScoped: true),
        new("tenancy", "Tenancy", "/tenancy", "/tenancy/acme", ShownToAdmin: false, TenantScoped: false),
        new("replication", "Replication", "/replication", "/replication/trees", ShownToAdmin: true, TenantScoped: true),
        new("backups", "Backups", "/backups", "/backups/schedules", ShownToAdmin: true, TenantScoped: true),
        new("telemetry", "Telemetry", "/telemetry", "/telemetry/overview", ShownToAdmin: false, TenantScoped: true),
        new("cluster", "Cluster", "/cluster", $"/cluster/trees/{ExplorerWorld.DemoTree}", ShownToAdmin: true, TenantScoped: false),
    ];

    /// <summary>The areas the test world shows its administrator.</summary>
    public static IEnumerable<ExplorerArea> Shown => All.Where(area => area.ShownToAdmin);

    /// <summary>The heading of the page the Explorer renders for an address nothing lives at.</summary>
    public const string NotFoundHeading = "Nothing lives at this address";

    /// <summary>The area with <paramref name="key"/>.</summary>
    /// <param name="key">The area key.</param>
    public static ExplorerArea Get(string key) => All.Single(area => area.Key == key);

    /// <summary>The keys of every area, for a test case source.</summary>
    public static IEnumerable<string> Keys() => All.Select(area => area.Key);
}
