using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// The Telemetry area's own static web assets, relative to the application's base
/// URL. Built from <see cref="ShellDesignAssets.ContentBasePath"/>, so the package
/// rename at cutover changes them in one place.
/// </summary>
internal static class TelemetryAssets
{
    /// <summary>The area's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "telemetry/";

    /// <summary>The area's stylesheet, which the head links after the design and chrome stylesheets.</summary>
    public const string Stylesheet = BasePath + "telemetry.css";
}
