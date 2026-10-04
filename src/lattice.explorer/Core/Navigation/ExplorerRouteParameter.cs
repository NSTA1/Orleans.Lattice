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
/// <see cref="ExplorerRouteSlug"/>, and must not be one of the shell's own
/// tenant-scope keys (<see cref="ExplorerRouteSegments.TenantQueryKey"/> or
/// <see cref="ExplorerRouteSegments.AllTenantsQueryKey"/>).
/// </param>
/// <param name="Value">
/// The raw value, escaped only when the route is formatted. May be empty, which
/// formats as a bare <c>?key=</c>.
/// </param>
/// <exception cref="ArgumentException">
/// <paramref name="Key"/> is not canonical lower case, or is one of the shell's
/// tenant-scope keys.
/// </exception>
public readonly record struct ExplorerRouteParameter(string Key, string Value)
{
    /// <summary>The query key. Always canonical lower case, and never a tenant-scope key.</summary>
    public string Key { get; } = ValidateKey(Key);

    /// <summary>The raw, unescaped value. Never <see langword="null"/>.</summary>
    public string Value { get; } = Value ?? string.Empty;

    /// <summary>
    /// Validates <paramref name="key"/> as an extension query key: canonical lower
    /// case, and not one of the shell's tenant-scope keys.
    /// </summary>
    /// <param name="key">The candidate key.</param>
    /// <param name="paramName">The caller's parameter name, for the exception.</param>
    /// <returns><paramref name="key"/>, unchanged.</returns>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not a valid extension key.</exception>
    internal static string ValidateKey(
        string key,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(key))] string? paramName = null)
    {
        ExplorerRouteSlug.EnsureCanonical(key, paramName);

        // The formatter writes the shell's own scope keys before the extension set
        // and the parser keeps the last occurrence, so an extension parameter under
        // either key would silently re-scope the route once it round-trips.
        if (string.Equals(key, ExplorerRouteSegments.TenantQueryKey, StringComparison.Ordinal) ||
            string.Equals(key, ExplorerRouteSegments.AllTenantsQueryKey, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"'{key}' is one of the Explorer shell's own tenant-scope query keys; set it with ExplorerRoute.WithTenant or ExplorerRoute.WithAllTenants instead.",
                paramName);
        }

        return key;
    }
}
