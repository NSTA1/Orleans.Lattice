using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The Data area's static assets, derived from the Shell's one content base path.</summary>
internal static class DataAssets
{
    /// <summary>The folder the Data area's assets are served from.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "data/";

    /// <summary>The Data area's stylesheet: <c>lt-data-*</c> classes only. The head links it after the chrome stylesheet.</summary>
    public const string Stylesheet = BasePath + "data.css";
}
