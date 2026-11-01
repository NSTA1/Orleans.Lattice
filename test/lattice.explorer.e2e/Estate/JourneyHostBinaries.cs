namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>The journey hosts the harness launches, and where their entry assemblies are.</summary>
internal static class JourneyHostBinaries
{
    /// <summary>
    /// A directory holding each host published to <c>{dir}/{project folder}/</c>, as the CI
    /// lane publishes them once per job. Unset, the harness launches the hosts from their
    /// build output beside this project, which building this project produces.
    /// </summary>
    public const string HostsDirectoryVariable = "LATTICE_E2E_HOSTS_DIR";

    /// <summary>The silo.</summary>
    public static string Silo => Locate("JourneySilo", "Orleans.Lattice.Explorer.EndToEnd.JourneySilo");

    /// <summary>The Explorer head.</summary>
    public static string Explorer => Locate("JourneyExplorer", "Orleans.Lattice.Explorer.EndToEnd.JourneyExplorer");

    /// <summary>The gateway.</summary>
    public static string Gateway => Locate("JourneyGateway", "Orleans.Lattice.Explorer.EndToEnd.JourneyGateway");

    /// <summary>The line feed.</summary>
    public static string LineFeed => Locate("LineFeed", "Orleans.Lattice.Explorer.EndToEnd.LineFeed");

    private static string Locate(string folder, string assembly)
    {
        var published = Environment.GetEnvironmentVariable(HostsDirectoryVariable);
        var candidate = string.IsNullOrWhiteSpace(published)
            ? Path.Combine(JourneyPaths.ProjectDirectory, "Hosts", folder, "bin", Configuration, "net10.0", assembly + ".dll")
            : Path.Combine(published, folder, assembly + ".dll");
        if (!File.Exists(candidate))
        {
            throw new FileNotFoundException(
                $"The journey host '{folder}' is not built at '{candidate}'. Build test/lattice.explorer.e2e (which builds its hosts), "
                + $"or publish them and set {HostsDirectoryVariable}.",
                candidate);
        }

        return candidate;
    }

    /// <summary>The configuration this test assembly was built in, read from its own output path.</summary>
    private static string Configuration
    {
        get
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            return directory.Parent?.Name ?? "Release";
        }
    }
}
