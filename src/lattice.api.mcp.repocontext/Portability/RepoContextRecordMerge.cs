using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="RepoContextSnapshotMerge"/> strategy: on import it
/// merges an incoming snapshot value into the value already stored under a key
/// through the record model's own CRDT join, so a re-import converges instead of
/// overwriting blindly.
/// <para>
/// The concrete record type is recovered from the key grammar (see
/// <see cref="RepoContextKeys.TryParse(string, out RepoContextKey)"/>): both the
/// existing and incoming value bytes are deserialized to that type, folded
/// through its static <c>Merge</c>, and re-serialized. A first-time import (no
/// existing value) or a key that does not parse to a known record family falls
/// back to storing the incoming bytes verbatim.
/// </para>
/// </summary>
internal static class RepoContextRecordMerge
{
    /// <summary>
    /// Builds the default merge strategy bound to <paramref name="serializer"/>.
    /// </summary>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode record values. Must not be <see langword="null"/>.</param>
    /// <returns>A merge strategy that folds through the record model's CRDT join.</returns>
    internal static RepoContextSnapshotMerge Default(Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return (key, existing, incoming) => Merge(serializer, key, existing, incoming);
    }

    private static byte[] Merge(Serializer serializer, string key, byte[]? existing, byte[] incoming)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null || !RepoContextKeys.TryParse(key, out var parsed))
        {
            return incoming;
        }

        return parsed.Kind switch
        {
            RepoContextRecordKind.Repo => Fold<RepoNode>(serializer, existing, incoming, RepoNode.Merge),
            RepoContextRecordKind.Package => Fold<PackageNode>(serializer, existing, incoming, PackageNode.Merge),
            RepoContextRecordKind.File => Fold<FileNode>(serializer, existing, incoming, FileNode.Merge),
            RepoContextRecordKind.Symbol => Fold<SymbolRecord>(serializer, existing, incoming, SymbolRecord.Merge),
            RepoContextRecordKind.Memory => Fold<MemoryRecord>(serializer, existing, incoming, MemoryRecord.Merge),
            _ => incoming,
        };
    }

    private static byte[] Fold<T>(
        Serializer serializer,
        byte[] existing,
        byte[] incoming,
        Func<T, T, T> merge)
    {
        var left = serializer.Deserialize<T>(existing);
        var right = serializer.Deserialize<T>(incoming);
        return serializer.SerializeToArray(merge(left, right));
    }
}
