namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Options for the local JSON config store. The <see cref="FilePath"/> is the
/// full path to the JSON document; each head supplies a per-user app-data
/// location (the MAUI app-data directory on Windows, the local application-data
/// folder on the web server).
/// </summary>
public sealed class ExplorerConfigStoreOptions
{
    /// <summary>The default config file name.</summary>
    public const string DefaultFileName = "config.json";

    /// <summary>The default per-user sub-folder the config lives under.</summary>
    public const string DefaultFolderName = "Orleans.Lattice.Explorer";

    /// <summary>The full path to the JSON config document.</summary>
    public string FilePath { get; set; } = DefaultFilePath();

    /// <summary>
    /// Builds the default config path under the per-user local application-data
    /// folder, for example
    /// <c>%LOCALAPPDATA%\Orleans.Lattice.Explorer\config.json</c> on Windows.
    /// </summary>
    public static string DefaultFilePath()
    {
        var root = Environment.GetFolderPath(
            Environment.SpecialFolder.LocalApplicationData,
            Environment.SpecialFolderOption.Create);
        return Path.Combine(root, DefaultFolderName, DefaultFileName);
    }
}
