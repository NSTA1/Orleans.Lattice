using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The frame host's own static web assets, relative to the application's base URL. Built
/// from <see cref="ShellDesignAssets.ContentBasePath"/>, so the package rename at cutover
/// changes them in one place.
/// </summary>
internal static class AppFrameAssets
{
    /// <summary>The frame host's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "appframe/";

    /// <summary>The host-side handshake and relay module.</summary>
    public const string HostModule = BasePath + "host.mjs";

    /// <summary>The host's layout stylesheet.</summary>
    public const string Stylesheet = BasePath + "appframe.css";

    /// <summary>The specifier the component imports the host module by, relative to the document's base URL.</summary>
    public const string HostModuleImport = "./" + HostModule;
}
