namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Encodes and decodes the set of fully-qualified symbol names a file declares as
/// the single newline-joined string held in a file node's
/// <see cref="FileNode.DeclaredSymbols"/> last-writer-wins register. The encoding is
/// deterministic - names are ordinal-sorted and de-duplicated - so an unchanged
/// declared set round-trips to the same bytes and never churns the register.
/// </summary>
internal static class DeclaredSymbolNames
{
    private const char Separator = '\n';

    /// <summary>
    /// Encodes <paramref name="fullyQualifiedNames"/> as a deterministic,
    /// newline-joined string (ordinal-sorted, de-duplicated, blanks dropped). An
    /// empty input encodes to the empty string.
    /// </summary>
    /// <param name="fullyQualifiedNames">The declared symbol names. Must not be
    /// <see langword="null"/>.</param>
    internal static string Encode(IEnumerable<string> fullyQualifiedNames)
    {
        ArgumentNullException.ThrowIfNull(fullyQualifiedNames);
        var ordered = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var name in fullyQualifiedNames)
        {
            if (!string.IsNullOrEmpty(name))
            {
                ordered.Add(name);
            }
        }

        return string.Join(Separator, ordered);
    }

    /// <summary>
    /// Decodes a newline-joined declared-symbol string back into its list of
    /// fully-qualified names. A <see langword="null"/> or empty input yields an empty
    /// list.
    /// </summary>
    /// <param name="encoded">The encoded value read from the register, or
    /// <see langword="null"/>.</param>
    internal static IReadOnlyList<string> Decode(string? encoded)
    {
        if (string.IsNullOrEmpty(encoded))
        {
            return [];
        }

        return encoded.Split(Separator, StringSplitOptions.RemoveEmptyEntries);
    }
}
