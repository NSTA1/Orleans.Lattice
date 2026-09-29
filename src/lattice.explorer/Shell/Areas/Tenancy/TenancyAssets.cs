using Orleans.Lattice.Explorer.Shell.Design;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// The Tenancy area's static web assets, derived from the Shell's one content
/// base path so the cutover rename changes them in one place.
/// </summary>
internal static class TenancyAssets
{
    /// <summary>The folder the area's assets are served from.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "tenancy/";

    /// <summary>The area's stylesheet, defining only <c>lt-tenancy-*</c> classes.</summary>
    public const string Stylesheet = BasePath + "lattice-tenancy.css";
}
