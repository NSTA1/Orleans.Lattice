namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// The local config backing store. Reads and writes the explorer's
/// <see cref="ExplorerConfiguration"/> as a JSON document. A missing or
/// malformed document is reported as "no configuration" rather than an error so
/// the app can fall back to the first-run configuration window.
/// </summary>
public interface IExplorerConfigStore
{
    /// <summary>The full path to the backing JSON document.</summary>
    string FilePath { get; }

    /// <summary><see langword="true"/> when the backing document exists on disk.</summary>
    bool Exists { get; }

    /// <summary>
    /// Loads the persisted configuration, or <see langword="null"/> when the
    /// document is missing or cannot be parsed.
    /// </summary>
    Task<ExplorerConfiguration?> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>Persists <paramref name="configuration"/>, creating the folder if needed.</summary>
    Task SaveAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default);
}
