using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The key layout of the approximate index plane, in one place: the exclusive
/// index prefix a <c>(repository, embedding space)</c> pair owns inside
/// <see cref="RepoContextTrees.VectorIndex"/>, the per-repository root those
/// prefixes are siblings under, and the coordinator grain key derived from the
/// pair.
/// <para>
/// <b>The repository sorts before the space, and that ordering is load-bearing.</b>
/// A prefix is <c>repo/{repoId}/vidx/{fingerprint}/</c>, so every embedding space
/// a repository has ever been indexed under is a sibling in one contiguous
/// ordinal range beneath <see cref="RepositoryRoot"/>. That is what makes it
/// possible to enumerate a repository's spaces - and therefore to retire the ones
/// a model change abandoned - with a bounded scan rather than a walk of the whole
/// index tree. Were the fingerprint to sort first, no repository-scoped scan would
/// exist and the abandoned spaces could only be found by remembering them.
/// </para>
/// </summary>
internal static class RepoContextAnnIndexKeys
{
    /// <summary>
    /// The separator between the repository and the space fingerprint in a build
    /// coordinator's grain key. The fingerprint is fixed-width hex and is the
    /// <i>last</i> segment, so a repository id containing the separator still
    /// parses unambiguously from the right.
    /// </summary>
    private const char GrainKeySeparator = '/';

    /// <summary>
    /// The stable fingerprint of an embedding space: a hash of its model,
    /// dimension, and normalization convention, so two spaces never share a prefix
    /// and no key ever carries a model id verbatim.
    /// </summary>
    /// <param name="space">The embedding space.</param>
    /// <returns>A 16-character lower-case hex fingerprint.</returns>
    internal static string SpaceFingerprint(EmbeddingSpaceTag space)
        => VectorCodec.SourceId($"{space.ModelId}|{space.Dimension}|{space.Normalization}");

    /// <summary>
    /// The key prefix under which every embedding space one repository has been
    /// indexed under is a sibling. Retiring a superseded space is a range delete
    /// inside this root; nothing outside it is ever in reach.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>The repository's index root prefix.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal static string RepositoryRoot(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return $"repo/{repoId}/{RepoContextAnnOptions.KeyPrefixRoot}";
    }

    /// <summary>
    /// The key prefix the index for one repository and embedding space owns
    /// exclusively. A durable index owns its prefix exclusively because its
    /// recovery path deletes whole key ranges under it.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The exclusive key prefix, ending in a separator.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal static string IndexPrefix(string repoId, EmbeddingSpaceTag space)
        => $"{RepositoryRoot(repoId)}{SpaceFingerprint(space)}/";

    /// <summary>
    /// The coordinator grain key for one repository and embedding space, mirroring
    /// the composite keys the tree coordinators use.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The grain key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal static string BuildGrainKey(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return $"{repoId}{GrainKeySeparator}{SpaceFingerprint(space)}";
    }

    /// <summary>
    /// Recovers the repository and the space fingerprint from a coordinator grain
    /// key. Parsed from the right, because the fingerprint is the fixed-width final
    /// segment and a repository id may itself contain the separator.
    /// </summary>
    /// <param name="grainKey">The grain key. Must not be <see langword="null"/>.</param>
    /// <param name="repoId">The repository the key names.</param>
    /// <param name="fingerprint">The embedding-space fingerprint the key names.</param>
    /// <returns><see langword="true"/> when both segments are non-empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grainKey"/> is null.</exception>
    internal static bool TryParseBuildGrainKey(
        string grainKey,
        [NotNullWhen(true)] out string? repoId,
        [NotNullWhen(true)] out string? fingerprint)
    {
        ArgumentNullException.ThrowIfNull(grainKey);
        repoId = null;
        fingerprint = null;

        var separator = grainKey.LastIndexOf(GrainKeySeparator);
        if (separator <= 0 || separator == grainKey.Length - 1)
        {
            return false;
        }

        repoId = grainKey[..separator];
        fingerprint = grainKey[(separator + 1)..];
        return true;
    }

    /// <summary>
    /// Given any key observed at or after a scan cursor inside
    /// <paramref name="repositoryRoot"/>, recovers the sibling space prefix that
    /// key belongs to - the root plus one fingerprint segment plus its trailing
    /// separator. This is what lets a reclamation walk skip a whole space in one
    /// hop instead of enumerating its records.
    /// </summary>
    /// <param name="repositoryRoot">The repository index root from <see cref="RepositoryRoot"/>. Must not be <see langword="null"/>.</param>
    /// <param name="key">The observed key. Must not be <see langword="null"/>.</param>
    /// <param name="spacePrefix">The sibling space prefix the key sits under.</param>
    /// <returns><see langword="false"/> when the key is not under the root, or
    /// carries no separated fingerprint segment - in which case it belongs to no
    /// space and must never be treated as one.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    internal static bool TrySpacePrefix(
        string repositoryRoot, string key, [NotNullWhen(true)] out string? spacePrefix)
    {
        ArgumentNullException.ThrowIfNull(repositoryRoot);
        ArgumentNullException.ThrowIfNull(key);
        spacePrefix = null;

        if (!key.StartsWith(repositoryRoot, StringComparison.Ordinal))
        {
            return false;
        }

        var separator = key.IndexOf('/', repositoryRoot.Length);
        if (separator < 0 || separator == repositoryRoot.Length)
        {
            // Either a key sitting directly under the root with no fingerprint
            // segment, or an empty fingerprint. Neither names a space, and treating
            // one as a space would compute a prefix that is the root itself - whose
            // range delete would take every space the repository has, live included.
            return false;
        }

        spacePrefix = key[..(separator + 1)];
        return true;
    }
}
