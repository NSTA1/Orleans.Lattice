namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Pure helpers for deriving unqualified names from the syntactic fully-qualified
/// names produced by <see cref="CSharpSymbolExtractor"/>, shared by the symbol
/// reconciler (which keys the reverse cross-reference projection by simple name) and
/// the graph service (which resolves a file's declared symbols to those same keys).
/// Keeping the derivation in one place ensures both sides agree on how a name maps to
/// a cross-reference key.
/// </summary>
internal static class SymbolNaming
{
    /// <summary>
    /// The unqualified type-name for a fully-qualified name: the final dot-separated
    /// segment with any generic arity marker (<c>&lt;...&gt;</c>) stripped, so
    /// <c>N.Outer&lt;T&gt;.FooTests</c> yields <c>FooTests</c>.
    /// </summary>
    /// <param name="fullyQualifiedName">The fully-qualified name. Must not be
    /// <see langword="null"/>.</param>
    internal static string SimpleName(string fullyQualifiedName)
    {
        ArgumentNullException.ThrowIfNull(fullyQualifiedName);
        var lastDot = fullyQualifiedName.LastIndexOf('.');
        var simple = lastDot < 0 ? fullyQualifiedName : fullyQualifiedName[(lastDot + 1)..];
        var generic = simple.IndexOf('<', StringComparison.Ordinal);
        return generic < 0 ? simple : simple[..generic];
    }

    /// <summary>
    /// The simple name of the type a test type covers by the <c>{X}Tests</c> /
    /// <c>{X}Test</c> naming convention, or <see langword="null"/> when the name does
    /// not match (or would leave an empty subject). For example <c>N.FooTests</c>
    /// yields <c>Foo</c>.
    /// </summary>
    /// <param name="fullyQualifiedName">The candidate test type's fully-qualified name.
    /// Must not be <see langword="null"/>.</param>
    internal static string? TestSubject(string fullyQualifiedName)
    {
        var simple = SimpleName(fullyQualifiedName);
        if (simple.Length > 5 && simple.EndsWith("Tests", StringComparison.Ordinal))
        {
            return simple[..^5];
        }

        if (simple.Length > 4 && simple.EndsWith("Test", StringComparison.Ordinal))
        {
            return simple[..^4];
        }

        return null;
    }
}
