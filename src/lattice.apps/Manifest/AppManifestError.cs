namespace Orleans.Lattice.Apps;

/// <summary>A path-addressed, machine-readable manifest diagnostic without exception transport.</summary>
/// <param name="Code">Stable diagnostic category.</param>
/// <param name="Path">JSON-style path to the invalid declaration.</param>
/// <param name="Message">Explanation suitable for an installation result.</param>
[GenerateSerializer, Alias(AppsTypeAliases.AppManifestError), Immutable]
public sealed record AppManifestError(
    [property: Id(0)] string Code,
    [property: Id(1)] string Path,
    [property: Id(2)] string Message);
