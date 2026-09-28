using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>Repository paths of the AppKit package and its fixtures.</summary>
internal static class AppKitPaths
{
    /// <summary>The AppKit project directory.</summary>
    public const string Project = "src/lattice.explorer/AppKit";

    /// <summary>The kit's versioned static-asset directory on disk.</summary>
    public const string AssetDirectory = Project + "/wwwroot/appkit/v1";

    /// <summary>The bootstrap document.</summary>
    public const string Frame = AssetDirectory + "/frame.html";

    /// <summary>The in-frame loader.</summary>
    public const string Boot = AssetDirectory + "/boot.js";

    /// <summary>The kit stylesheet.</summary>
    public const string Stylesheet = AssetDirectory + "/lattice-app.css";

    /// <summary>The example bundle directory.</summary>
    public const string Example = Project + "/example";

    /// <summary>The browser-lane fixture directory.</summary>
    public const string Fixtures = "test/lattice.explorer/AppKit/Fixtures";

    /// <summary>The AppKit package's static-web-asset base path.</summary>
    public const string BasePath = "_content/Orleans.Lattice.Explorer.AppKit";

    /// <summary>Resolves a repository-relative path.</summary>
    public static string Absolute(string relative) =>
        Path.Combine(HygieneRepository.FindRepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>Reads a repository-relative text file.</summary>
    public static string Read(string relative) => File.ReadAllText(Absolute(relative));
}
