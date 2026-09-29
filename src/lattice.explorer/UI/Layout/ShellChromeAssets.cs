using Orleans.Lattice.Explorer.UI.Design;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The navigation chrome's static web assets, relative to the application's base
/// path. Built from <see cref="ShellDesignAssets.ContentBasePath"/>, the one place
/// that names the package's asset root, so a rename changes one constant.
/// </summary>
/// <remarks>
/// A head links <see cref="Stylesheet"/> after <see cref="ShellDesignAssets.Stylesheets"/>
/// and puts <see cref="FirstPaintScript"/> in <c>&lt;head&gt;</c> as a classic,
/// blocking script (no <c>defer</c> or <c>async</c>), so the chosen material is on
/// the document before the first paint. The chrome imports <see cref="Module"/> itself.
/// </remarks>
internal static class ShellChromeAssets
{
    /// <summary>The chrome's folder within the Shell's static web assets.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "shell/";

    /// <summary>The chrome's stylesheet: the frame, spine, address line, palette and Home.</summary>
    public const string Stylesheet = BasePath + "lattice-chrome.css";

    /// <summary>The chrome's JavaScript module: keyboard shortcuts, appearance, focus, and the width band.</summary>
    public const string Module = BasePath + "lattice-chrome.js";

    /// <summary>The classic script that applies the remembered appearance before the first paint.</summary>
    public const string FirstPaintScript = BasePath + "lattice-appearance.js";

    /// <summary>The specifier the chrome imports <see cref="Module"/> by, resolved against the document's base.</summary>
    public const string ModuleSpecifier = "./" + Module;
}
