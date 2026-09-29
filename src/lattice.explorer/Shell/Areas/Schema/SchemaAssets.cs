using Orleans.Lattice.Explorer.Shell.Design;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The Schema area's own static web assets, relative to the application's base
/// URL. Built from <see cref="ShellDesignAssets.ContentBasePath"/>, so the package
/// rename at cutover changes them in one place.
/// </summary>
internal static class SchemaAssets
{
    /// <summary>The area's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "schema/";

    /// <summary>The area's stylesheet: headings, meta lines, rule lists, staged operations and notes.</summary>
    public const string Stylesheet = BasePath + "lattice-schema.css";
}
