using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps catalogue's static assets, derived from the one constant that names the
/// package's asset root, so the cutover's rename changes nothing here.
/// </summary>
internal static class AppsCatalogueAssets
{
    /// <summary>The Apps area's asset folder.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "apps/";

    /// <summary>The catalogue, review and lifecycle stylesheet; the Apps pages link it themselves.</summary>
    public const string Stylesheet = BasePath + "catalogue.css";
}
