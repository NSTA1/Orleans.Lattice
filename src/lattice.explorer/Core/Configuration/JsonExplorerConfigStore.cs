using System.Text.Json;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// A <see cref="IExplorerConfigStore"/> backed by a local JSON file. Writes are
/// atomic (temp file then move) and a missing or corrupt document loads as
/// <see langword="null"/> so the app re-prompts instead of crashing.
/// </summary>
public sealed class JsonExplorerConfigStore : IExplorerConfigStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
    };

    private readonly ExplorerConfigStoreOptions _options;

    /// <summary>Creates a store over the path in <paramref name="options"/>.</summary>
    public JsonExplorerConfigStore(ExplorerConfigStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public string FilePath => _options.FilePath;

    /// <inheritdoc />
    public bool Exists => File.Exists(_options.FilePath);

    /// <inheritdoc />
    public async Task<ExplorerConfiguration?> LoadAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_options.FilePath))
        {
            return null;
        }

        try
        {
            await using var stream = File.OpenRead(_options.FilePath);
            return await JsonSerializer.DeserializeAsync<ExplorerConfiguration>(
                stream, SerializerOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            // A corrupt or unreadable document is treated as "no configuration"
            // so the app falls back to the first-run window rather than failing.
            return null;
        }
    }

    /// <inheritdoc />
    public async Task SaveAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var directory = Path.GetDirectoryName(_options.FilePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var tempPath = _options.FilePath + ".tmp";
        await using (var stream = File.Create(tempPath))
        {
            await JsonSerializer.SerializeAsync(stream, configuration, SerializerOptions, cancellationToken).ConfigureAwait(false);
        }

        File.Move(tempPath, _options.FilePath, overwrite: true);
    }
}
