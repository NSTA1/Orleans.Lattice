namespace Orleans.Lattice.Apps;

/// <summary>A local parse or validation outcome. Failed outcomes never expose a usable manifest.</summary>
public sealed class AppManifestResult
{
    internal AppManifestResult(AppManifest? manifest, List<AppManifestError> errors)
    {
        Manifest = errors.Count == 0 ? manifest : null;
        Errors = errors.AsReadOnly();
    }

    /// <summary>Whether parsing and all requested validation completed successfully.</summary>
    public bool IsValid => Manifest is not null && Errors.Count == 0;

    /// <summary>The manifest on success, otherwise null.</summary>
    public AppManifest? Manifest { get; }

    /// <summary>Read-only diagnostics; empty on success.</summary>
    public IReadOnlyList<AppManifestError> Errors { get; }

    internal static AppManifestResult Failure(string code, string path, string message) =>
        new(null, [new(code, path, message)]);
}
