namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>Resolves repository-relative paths from the test binary's location.</summary>
internal static class RepositoryPaths
{
    private static readonly Lazy<string> Root = new(FindRoot);

    /// <summary>The repository root: the directory holding <c>Orleans.Lattice.slnx</c>.</summary>
    public static string RepositoryRoot => Root.Value;

    /// <summary>The absolute path of a repository-relative <paramref name="path"/>.</summary>
    /// <param name="path">A path such as <c>test/lattice.explorer/AppKit/Fixtures</c>.</param>
    public static string Resolve(string path) =>
        Path.GetFullPath(Path.Combine(RepositoryRoot, path.Replace('/', Path.DirectorySeparatorChar)));

    private static string FindRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Orleans.Lattice.slnx")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException(
            $"No directory above '{AppContext.BaseDirectory}' holds Orleans.Lattice.slnx, so the suite cannot find the fixtures it reads from the repository.");
    }
}
