using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The Backups area's static web assets, relative to the application's base
/// path. Built from <see cref="ShellDesignAssets.ContentBasePath"/>, the one
/// place that names the package's asset root.
/// </summary>
/// <remarks>
/// A head links <see cref="Stylesheet"/> after the chrome's stylesheet. The area
/// imports <see cref="Module"/> itself, and only to save an exported artifact.
/// </remarks>
internal static class BackupsAssets
{
    /// <summary>The area's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "backups/";

    /// <summary>The area's stylesheet: only <c>lt-backups-*</c> classes.</summary>
    public const string Stylesheet = BasePath + "lattice-backups.css";

    /// <summary>The area's JavaScript module: saving an exported artifact as a download.</summary>
    public const string Module = BasePath + "lattice-backups.js";

    /// <summary>The specifier the area imports <see cref="Module"/> by, resolved against the document's base.</summary>
    public const string ModuleSpecifier = "./" + Module;
}
