namespace Orleans.Lattice.Backup;

/// <summary>
/// A reference to the compression dictionary in force for a captured backup, so a
/// restore can locate and re-hydrate dictionary-compressed artifact content. The
/// dictionary bytes themselves are not embedded in the manifest - only an
/// identity and a digest that a sink or a dictionary store resolves.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupCompressionDictionaryRef)]
[Immutable]
public sealed record BackupCompressionDictionaryRef
{
    /// <summary>Initializes a new <see cref="BackupCompressionDictionaryRef"/>.</summary>
    /// <param name="dictionaryId">The compression dictionary identity. Must not be <c>null</c> or empty.</param>
    /// <param name="digest">The content digest of the dictionary bytes. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="dictionaryId"/> or <paramref name="digest"/> is <c>null</c> or empty.</exception>
    public BackupCompressionDictionaryRef(string dictionaryId, string digest)
    {
        ArgumentException.ThrowIfNullOrEmpty(dictionaryId);
        ArgumentException.ThrowIfNullOrEmpty(digest);
        DictionaryId = dictionaryId;
        Digest = digest;
    }

    /// <summary>The compression dictionary identity.</summary>
    [Id(0)]
    public string DictionaryId { get; init; }

    /// <summary>The content digest of the dictionary bytes.</summary>
    [Id(1)]
    public string Digest { get; init; }
}
