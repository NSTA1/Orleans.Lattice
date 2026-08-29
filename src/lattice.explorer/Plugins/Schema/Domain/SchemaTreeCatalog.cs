namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// The result of listing the trees the Schema area can govern: the projected
/// trees, or the message explaining why discovery failed.
/// <para>
/// Discovery folds a fault into <see cref="Error"/> rather than throwing, in
/// the same shape as every other Schema read, so the selection list can offer a
/// retry instead of the panel surfacing an unhandled exception.
/// </para>
/// </summary>
public sealed record SchemaTreeCatalog
{
    private static readonly SchemaTreeSummary[] NoTrees = [];

    /// <summary>The empty, successful catalog.</summary>
    public static SchemaTreeCatalog Empty { get; } = new();

    /// <summary>The governable trees, in catalog order. Empty when discovery failed.</summary>
    public IReadOnlyList<SchemaTreeSummary> Trees { get; init; } = NoTrees;

    /// <summary>
    /// The discovery failure message, or <see langword="null"/> when the listing
    /// succeeded (including when it legitimately found no trees).
    /// </summary>
    public string? Error { get; init; }

    /// <summary><see langword="true"/> when discovery completed.</summary>
    public bool IsSuccess => Error is null;

    /// <summary>Creates a successful catalog carrying <paramref name="trees"/>.</summary>
    /// <param name="trees">The projected trees. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="trees"/> is <see langword="null"/>.</exception>
    public static SchemaTreeCatalog Succeeded(IReadOnlyList<SchemaTreeSummary> trees)
    {
        ArgumentNullException.ThrowIfNull(trees);
        return new SchemaTreeCatalog { Trees = trees };
    }

    /// <summary>Creates a failed catalog carrying <paramref name="message"/>.</summary>
    /// <param name="message">The discovery failure message. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static SchemaTreeCatalog Failed(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new SchemaTreeCatalog { Error = message };
    }
}
