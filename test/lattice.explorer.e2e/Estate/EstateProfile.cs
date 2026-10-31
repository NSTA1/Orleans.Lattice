namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// The two estates of the epic's decision record (D1, D2), as <see cref="JourneyEstateOptions"/>
/// ready for <see cref="JourneyEstate.StartAsync"/>. Each property returns a fresh instance, so a
/// caller may extend its lists (the persona catalogue adds users) without touching another
/// estate's.
/// </summary>
public static class EstateProfile
{
    /// <summary>The Fabrikam bootstrap administrator: the hosting operator.</summary>
    public static string FabrikamAdministrator => Cast.Personas.Hal.Id;

    /// <summary>
    /// Contoso, non-tenanted manufacturing: west (two silos behind a gateway, one Explorer that
    /// persists no keys, and the line feed), east (one silo on its own storage account, two
    /// console replicas under <c>/console</c> sharing a Blob-persisted key ring, and
    /// <c>factory-lines</c> enrolled statically), the two peered over replication, Prometheus
    /// scraping every silo, and the <c>contoso-kiosk</c> head seeded with the platform
    /// operator's credential.
    /// </summary>
    public static JourneyEstateOptions Contoso => new()
    {
        Name = "contoso",
        East = true,
        Prometheus = true,
        KioskUser = JourneyEstateOptions.Administrator,
        EastStaticReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            ["factory-lines"] = LatticeMergeMode.LwwRegister,
        },
    };

    /// <summary>
    /// Fabrikam Cloud, a tenanted hosting provider: west and east regions shaped as Contoso's,
    /// with tenancy and delegated-access administration on (so the tenant-admin facades are
    /// served), Prometheus, Explorer heads whose connection settings the browser may edit, no
    /// line feed and no kiosk. Its directory holds the Fabrikam cast (<see cref="Cast.Personas.Fabrikam"/>),
    /// and <c>hal</c> is the bootstrap administrator.
    /// </summary>
    public static JourneyEstateOptions Fabrikam => new()
    {
        Name = "fabrikam",
        East = true,
        Prometheus = true,
        Tenancy = true,
        DelegatedAccessAdministration = true,
        AllowInteractiveEndpointConfiguration = true,
        LineFeed = false,
        Users = Cast.Personas.FabrikamUsers(),
        BootstrapAdministrators = Cast.Personas.BootstrapAdministratorsOf(Cast.Personas.FabrikamEstate),
    };
}
