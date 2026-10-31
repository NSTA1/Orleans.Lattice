namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The bounds a head sets on imports (issue #5365). A web head registers one from
/// <c>LatticeExplorerWebOptions.MaxImportBytes</c>; a head that registers none gets
/// <see cref="Default"/>.
/// </summary>
internal sealed record DataImportLimits
{
    /// <summary>The default largest file an import reads: 2 GiB.</summary>
    public const long DefaultMaxImportBytes = 2L * 1024 * 1024 * 1024;

    /// <summary>The limits a head that registers none gets.</summary>
    public static DataImportLimits Default { get; } = new();

    /// <summary>The largest file, in bytes, an import reads.</summary>
    public long MaxImportBytes { get; init; } = DefaultMaxImportBytes;
}
