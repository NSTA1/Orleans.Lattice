namespace Orleans.Lattice.Backup;

/// <summary>
/// The captured shape of a single key: its declared conflict-resolution mode and
/// the origin that last wrote it. Recorded so a restore is mode-faithful -
/// re-applying a last-writer-wins register and a CRDT through their respective
/// paths - even for a mixed-mode tree.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupKeyDescriptor)]
[Immutable]
public sealed record BackupKeyDescriptor
{
    /// <summary>Initializes a new <see cref="BackupKeyDescriptor"/>.</summary>
    /// <param name="key">The captured key. Must not be <c>null</c> or empty.</param>
    /// <param name="mergeMode">The declared conflict-resolution mode of the key.</param>
    /// <param name="originId">
    /// The origin that last wrote the key, or <c>null</c> when the tree is
    /// single-origin (local-only).
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="key"/> is <c>null</c> or empty.</exception>
    public BackupKeyDescriptor(string key, BackupKeyMergeMode mergeMode, string? originId = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        Key = key;
        MergeMode = mergeMode;
        OriginId = originId;
    }

    /// <summary>The captured key.</summary>
    [Id(0)]
    public string Key { get; init; }

    /// <summary>The declared conflict-resolution mode of the key.</summary>
    [Id(1)]
    public BackupKeyMergeMode MergeMode { get; init; }

    /// <summary>The origin that last wrote the key, or <c>null</c> for a single-origin tree.</summary>
    [Id(2)]
    public string? OriginId { get; init; }
}
