namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The route templates the navigation chrome itself owns. Every one is lower
/// case and begins with a literal segment (<c>RouteCaseHygieneTests</c>).
/// </summary>
internal static class ExplorerRoutes
{
    /// <summary>The segment of the not-found page, reserved from area keys.</summary>
    public const string NotFoundSegment = "not-found";

    /// <summary>Home.</summary>
    public const string Home = "/";

    /// <summary>A tenant's Home, when tenancy is on.</summary>
    public const string TenantHome = "/t/{tenant}";

    /// <summary>The not-found page, which the head passes to the router as its not-found page.</summary>
    public const string NotFound = "/" + NotFoundSegment;
}
