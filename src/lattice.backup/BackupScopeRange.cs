namespace Orleans.Lattice.Backup;

/// <summary>
/// Maps a <see cref="BackupScopeSelector"/> to the half-open key range it covers.
/// Capture and restore both resolve scopes through this one mapping, so a restore
/// filters exactly the range its capture streamed.
/// </summary>
internal static class BackupScopeRange
{
    /// <summary>
    /// Maps a scope to its half-open key range: whole-tree is unbounded, a prefix is
    /// [prefix, prefixUpperBound), a single key is [key, key + separator).
    /// </summary>
    /// <param name="scope">The scope to resolve.</param>
    /// <exception cref="ArgumentOutOfRangeException">The scope kind is not recognised.</exception>
    internal static (string? startInclusive, string? endExclusive) Resolve(BackupScopeSelector scope) =>
        scope.Kind switch
        {
            BackupScopeKind.WholeTree => (null, null),
            BackupScopeKind.Prefix => (scope.KeyOrPrefix, BackupConstants.PrefixUpperBound(scope.KeyOrPrefix!)),
            BackupScopeKind.Key => (scope.KeyOrPrefix, scope.KeyOrPrefix + "\0"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope.Kind, "Unknown backup scope kind."),
        };
}
