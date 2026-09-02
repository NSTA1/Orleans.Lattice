namespace Orleans.Lattice.Backup.AzureBlob;

/// <summary>
/// Deterministic blob-name layout for the Azure Blob backup sink. Manifests and
/// artifacts live under distinct, lexicographically ordered prefixes so listing
/// or reading a chain is a single ordered prefix scan:
/// <list type="bullet">
/// <item><description><c>manifests/{backupId}</c> - one block blob per manifest, keyed by backup id.</description></item>
/// <item><description><c>artifacts/{artifactId}</c> - one append blob per content-addressed artifact.</description></item>
/// </list>
/// Because Azure Blob Storage returns listings in lexicographical name order,
/// listing a prefix yields ids in id order, matching the ordering the
/// <see cref="ILatticeBackupSink"/> contract requires.
/// </summary>
/// <remarks>
/// An id may legitimately contain a <c>/</c> - a tenant-composed tree id of the
/// form <c>t/{tenant}/{name}</c> is embedded verbatim in every artifact id - so
/// the separator itself cannot be rejected. What must be rejected is any id that
/// changes the blob's *resolved* location: the Azure SDK builds a blob URI
/// through <see cref="UriBuilder"/>/<see cref="Uri"/>, which performs RFC 3986
/// dot-segment removal after percent-decoding, so an id containing a <c>..</c>
/// segment (raw or percent-encoded) resolves above the manifest or artifact
/// prefix and can escape the configured container entirely. Every id is
/// therefore validated by <see cref="ValidateId"/> before it is concatenated
/// onto a prefix.
/// </remarks>
internal static class BackupBlobNaming
{
    /// <summary>The blob-name prefix (including trailing slash) under which manifests are stored.</summary>
    internal const string ManifestPrefix = "manifests/";

    /// <summary>The blob-name prefix (including trailing slash) under which artifacts are stored.</summary>
    internal const string ArtifactPrefix = "artifacts/";

    /// <summary>
    /// Blob metadata key set to <c>"true"</c> once every chunk of an artifact has
    /// been appended. A partially written append blob (created but not yet
    /// committed) is therefore distinguishable from a complete one, so a retried
    /// write overwrites it rather than treating it as an idempotent no-op.
    /// </summary>
    internal const string CommittedMetadataKey = "committed";

    /// <summary>The committed-metadata value written once an artifact is complete.</summary>
    internal const string CommittedMetadataValue = "true";

    /// <summary>Returns the block-blob name for a manifest keyed by <paramref name="backupId"/>.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty, and must be a relative path free of dot segments.</param>
    /// <returns>The manifest blob name.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c>, empty, or would resolve outside <see cref="ManifestPrefix"/>.</exception>
    internal static string ManifestBlobName(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ValidateId(backupId, nameof(backupId));
        return string.Concat(ManifestPrefix, backupId);
    }

    /// <summary>Returns the append-blob name for an artifact keyed by <paramref name="artifactId"/>.</summary>
    /// <param name="artifactId">The content-addressed artifact id. Must not be <c>null</c> or empty, and must be a relative path free of dot segments.</param>
    /// <returns>The artifact blob name.</returns>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> is <c>null</c>, empty, or would resolve outside <see cref="ArtifactPrefix"/>.</exception>
    internal static string ArtifactBlobName(string artifactId)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ValidateId(artifactId, nameof(artifactId));
        return string.Concat(ArtifactPrefix, artifactId);
    }

    /// <summary>
    /// Rejects any id that would not resolve to a blob strictly beneath the
    /// prefix it is concatenated onto. A <c>/</c> is permitted because a
    /// tenant-composed tree id embeds one, but each resulting segment must be a
    /// non-empty name that is neither <c>.</c> nor <c>..</c>, since
    /// <see cref="Uri"/> removes those segments when the Azure SDK resolves the
    /// blob address.
    /// </summary>
    /// <remarks>
    /// The id is checked both as written and after a single percent-decode,
    /// because <see cref="Uri"/> performs dot-segment removal after
    /// percent-decoding: <c>%2E%2E/secrets</c> resolves exactly as
    /// <c>../secrets</c> does. One decode matches the platform, which does not
    /// decode a second time, so a double-encoded id is inert. A backslash is
    /// rejected in both forms because <see cref="Uri"/> normalises it to a
    /// separator, which would otherwise reintroduce the escape through a
    /// different spelling. Control characters are rejected because they are not
    /// valid in a blob name and can be used to confuse logging and downstream
    /// parsing.
    /// </remarks>
    /// <param name="id">The caller-influenced id being placed under a prefix.</param>
    /// <param name="paramName">The originating parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException">The id would escape its prefix or is otherwise not a valid blob-name suffix.</exception>
    private static void ValidateId(string id, string paramName)
    {
        ValidateForm(id, paramName);

        string decoded;
        try
        {
            decoded = Uri.UnescapeDataString(id);
        }
        catch (UriFormatException)
        {
            throw new ArgumentException(
                "A backup id must not contain a malformed percent-escape.",
                paramName);
        }

        if (!string.Equals(decoded, id, StringComparison.Ordinal))
        {
            ValidateForm(decoded, paramName);
        }
    }

    /// <summary>
    /// Applies the segment and character rules to one spelling of an id.
    /// </summary>
    /// <param name="id">The id, either as written or percent-decoded.</param>
    /// <param name="paramName">The originating parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException">The id would escape its prefix or is otherwise not a valid blob-name suffix.</exception>
    private static void ValidateForm(string id, string paramName)
    {
        if (id.Length == 0 || id[0] == '/')
        {
            throw new ArgumentException(
                "A backup id must be a non-empty relative blob-name suffix and must not start with '/'.",
                paramName);
        }

        foreach (var c in id)
        {
            if (c == '\\' || char.IsControl(c))
            {
                throw new ArgumentException(
                    "A backup id must not contain a backslash or a control character.",
                    paramName);
            }
        }

        foreach (var segment in id.Split('/'))
        {
            if (segment.Length == 0 || segment is "." or "..")
            {
                throw new ArgumentException(
                    "A backup id must not contain an empty, '.', or '..' path segment, because the blob address would resolve outside its prefix.",
                    paramName);
            }
        }
    }

    /// <summary>
    /// Recovers the backup id from a manifest blob name, or <c>null</c> when the
    /// name does not sit under <see cref="ManifestPrefix"/>.
    /// </summary>
    /// <param name="blobName">The full blob name.</param>
    /// <returns>The backup id, or <c>null</c>.</returns>
    internal static string? BackupIdFromManifestBlobName(string blobName) =>
        blobName is not null && blobName.StartsWith(ManifestPrefix, StringComparison.Ordinal)
            ? blobName[ManifestPrefix.Length..]
            : null;

    /// <summary>
    /// Recovers the artifact id from an artifact blob name, or <c>null</c> when the
    /// name does not sit under <see cref="ArtifactPrefix"/>.
    /// </summary>
    /// <param name="blobName">The full blob name.</param>
    /// <returns>The artifact id, or <c>null</c>.</returns>
    internal static string? ArtifactIdFromBlobName(string blobName) =>
        blobName is not null && blobName.StartsWith(ArtifactPrefix, StringComparison.Ordinal)
            ? blobName[ArtifactPrefix.Length..]
            : null;
}
