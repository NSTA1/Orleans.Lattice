using Orleans.Lattice.Explorer.Shell.Design;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// The Replication area's own static web assets, relative to the application's base
/// URL. Built from <see cref="ShellDesignAssets.ContentBasePath"/>, so the package
/// rename at cutover changes them in one place.
/// </summary>
internal static class ReplicationAssets
{
    /// <summary>The area's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "replication/";

    /// <summary>The area's stylesheet: the estate diagram, section links and row notes.</summary>
    public const string Stylesheet = BasePath + "lattice-replication.css";

    /// <summary>The page-visibility module the refresh cadence reads.</summary>
    public const string Module = BasePath + "lattice-replication.js";

    /// <summary>The specifier the visibility observer imports the module by, relative to the document's base URL.</summary>
    public const string ModuleSpecifier = "./" + Module;
}
