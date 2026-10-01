using System.Globalization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Records the exact scopes a backup operation was authorized over on its tracked
/// operation, and reads them back, so a later status read, listing or cancel can
/// be authorized against the same scopes rather than a widened whole-tree check.
/// The tree of scope <c>i</c> is the operation's tree <c>i</c>; a whole-tree scope
/// needs no attribute, and a prefix or key scope records its kind and key.
/// </summary>
internal static class BackupOperationScopes
{
    private const string AttributePrefix = "scope.";
    private const string KindSuffix = ".kind";
    private const string KeySuffix = ".key";

    private static readonly IReadOnlyDictionary<string, string> NoAttributes =
        new Dictionary<string, string>(0, StringComparer.Ordinal);

    /// <summary>The tree ids of <paramref name="scopes"/>, in order.</summary>
    /// <param name="scopes">The authorized scopes.</param>
    /// <returns>The tree ids.</returns>
    internal static IReadOnlyList<string> TreeIds(IReadOnlyList<BackupScopeSelector> scopes)
    {
        var trees = new string[scopes.Count];
        for (var i = 0; i < trees.Length; i++)
        {
            trees[i] = scopes[i].TreeId;
        }

        return trees;
    }

    /// <summary>Encodes the sub-tree shape of <paramref name="scopes"/> as operation attributes.</summary>
    /// <param name="scopes">The authorized scopes.</param>
    /// <returns>The attributes; empty when every scope is whole-tree.</returns>
    internal static IReadOnlyDictionary<string, string> ToAttributes(IReadOnlyList<BackupScopeSelector> scopes)
    {
        Dictionary<string, string>? attributes = null;
        for (var i = 0; i < scopes.Count; i++)
        {
            var scope = scopes[i];
            if (scope.Kind == BackupScopeKind.WholeTree)
            {
                continue;
            }

            attributes ??= new Dictionary<string, string>(StringComparer.Ordinal);
            var index = i.ToString(CultureInfo.InvariantCulture);
            attributes[AttributePrefix + index + KindSuffix] = scope.Kind.ToString();
            attributes[AttributePrefix + index + KeySuffix] = scope.KeyOrPrefix ?? string.Empty;
        }

        return attributes ?? NoAttributes;
    }

    /// <summary>
    /// Rebuilds the authorized scopes from an operation's trees and attributes, or
    /// returns <see langword="null"/> when the attributes are malformed - which a
    /// caller must treat as not authorized (fail closed).
    /// </summary>
    /// <param name="treeIds">The operation's tree ids.</param>
    /// <param name="attributes">The operation's attributes.</param>
    /// <returns>The scopes, or <see langword="null"/>.</returns>
    internal static IReadOnlyList<BackupScopeSelector>? FromOperation(
        IReadOnlyList<string> treeIds,
        IReadOnlyDictionary<string, string> attributes)
    {
        if (treeIds.Count == 0)
        {
            return null;
        }

        var scopes = new BackupScopeSelector[treeIds.Count];
        for (var i = 0; i < scopes.Length; i++)
        {
            var index = i.ToString(CultureInfo.InvariantCulture);
            if (!attributes.TryGetValue(AttributePrefix + index + KindSuffix, out var kindText))
            {
                scopes[i] = BackupScopeSelector.WholeTree(treeIds[i]);
                continue;
            }

            if (!Enum.TryParse<BackupScopeKind>(kindText, ignoreCase: false, out var kind)
                || kind == BackupScopeKind.WholeTree
                || !attributes.TryGetValue(AttributePrefix + index + KeySuffix, out var key)
                || string.IsNullOrEmpty(key))
            {
                return null;
            }

            scopes[i] = new BackupScopeSelector(kind, treeIds[i], key);
        }

        return scopes;
    }
}
