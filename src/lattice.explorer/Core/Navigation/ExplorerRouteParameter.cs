namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// One extra query parameter carried on an <see cref="ExplorerRoute"/>: a
/// canonical lower-case <see cref="Key"/> and its raw (unescaped)
/// <see cref="Value"/>.
/// </summary>
/// <remarks>
/// The shell's own tenant-scope keys are declared on
/// <see cref="ExplorerRouteSegments"/>. This type is the extension point for
/// everything else: a surface that needs its own state in the URL adds a
/// parameter here rather than editing the shell's route grammar.
/// </remarks>
/// <param name="Key">
/// The query key. Must be canonical (lower case) per
/// <see cref="ExplorerRouteSlug"/>.
/// </param>
/// <param name="Value">
/// The raw value, escaped only when the route is formatted. May be empty, which
/// formats as a bare <c>?key=</c>.
/// </param>
public readonly record struct ExplorerRouteParameter(string Key, string Value)
{
    /// <summary>The query key. Always canonical lower case.</summary>
    public string Key { get; } = Validated(Key);

    /// <summary>The raw, unescaped value. Never <see langword="null"/>.</summary>
    public string Value { get; } = Value ?? string.Empty;

    private static string Validated(string key)
    {
        ExplorerRouteSlug.EnsureCanonical(key);
        return key;
    }
}
