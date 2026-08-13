namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Fail-fast guard for the durable data paths that must live on a host mount: the
/// WAL directory and the SQLite database directory. It refuses to let the
/// container start against a data path that is missing and uncreatable, or that
/// the non-root runtime UID cannot write to, so a misconfigured mount surfaces
/// immediately instead of silently losing durability.
/// </summary>
public static class DataPathGuard
{
    /// <summary>
    /// Ensures <paramref name="directory"/> exists (creating it if needed) and is
    /// writable by the current process UID, throwing otherwise.
    /// </summary>
    /// <param name="directory">The directory that must exist and be writable.</param>
    /// <param name="purpose">A short description used in the failure message (e.g. "WAL").</param>
    /// <exception cref="ArgumentException"><paramref name="directory"/> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">The directory cannot be created or written to.</exception>
    public static void EnsureDirectoryWritable(string directory, string purpose)
    {
        if (string.IsNullOrWhiteSpace(directory))
        {
            throw new ArgumentException("The directory path must not be empty.", nameof(directory));
        }

        var full = Path.GetFullPath(directory);

        try
        {
            Directory.CreateDirectory(full);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                $"The {purpose} directory '{full}' could not be created. Ensure the data root is a host mount "
                + "writable by the container's non-root UID.", ex);
        }

        var probe = Path.Combine(full, $".repocontext-write-probe-{Guid.NewGuid():N}");
        try
        {
            File.WriteAllText(probe, "ok");
            File.Delete(probe);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                $"The {purpose} directory '{full}' is not writable by the container's non-root UID. "
                + "Mount a writable volume at the data root.", ex);
        }
    }
}
