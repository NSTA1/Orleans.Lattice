using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Builders and a parser for the repository-context key grammar - the stable,
/// hierarchical mapping of a codebase's structure and an agent's accumulated
/// knowledge onto the ordered <c>string -&gt; byte[]</c> Lattice keyspace, so
/// that prefix and range scans are the natural query primitive.
/// <para>
/// <b>Grammar</b> (all keys are rooted at the <c>repo/</c> segment):
/// </para>
/// <list type="bullet">
///   <item><description><c>repo/{repoId}</c> - a repository root node.</description></item>
///   <item><description><c>repo/{repoId}/pkg/{path}</c> - a package / module / directory node.</description></item>
///   <item><description><c>repo/{repoId}/file/{path}</c> - a source-file node.</description></item>
///   <item><description><c>repo/{repoId}/symbol/{fqName}</c> - a symbol record.</description></item>
///   <item><description><c>repo/{repoId}/mem/{topic}/{id}</c> - an agent memory record.</description></item>
///   <item><description><c>repo/{repoId}/vec/{vectorId}</c> - a vector metadata record.</description></item>
///   <item><description><c>repo/{repoId}/vpay/{contentAddress}</c> - a content-addressed vector payload.</description></item>
///   <item><description><c>repo/{repoId}/vmem/{collection}</c> - a vector collection membership record.</description></item>
/// </list>
/// <para>
/// <b>Encoding.</b> Opaque single components (<c>repoId</c>, <c>topic</c>, and
/// the memory <c>id</c>) percent-encode both <c>'%'</c> and <c>'/'</c>, so they
/// can never introduce a stray segment boundary. Hierarchical components (the
/// file / package <c>path</c>) percent-encode only <c>'%'</c> and preserve
/// <c>'/'</c>, so a directory subtree stays contiguous under an ordered range
/// scan (<c>repo/{repoId}/file/{dir}/</c> is a single prefix range). The symbol
/// <c>fqName</c> is treated as an opaque component (dots are preserved, so a
/// namespace prefix scan works). Because only <c>'%'</c> and <c>'/'</c> are ever
/// escaped and both are ASCII, the encoding is fully reversible.
/// </para>
/// <para>
/// The grammar is effectively wire format for stored data: never change the
/// segment tokens or the encoding once data exists.
/// </para>
/// </summary>
internal static class RepoContextKeys
{
    /// <summary>The root segment token.</summary>
    internal const string RepoSegment = "repo";

    /// <summary>The package segment token.</summary>
    internal const string PackageSegment = "pkg";

    /// <summary>The file segment token.</summary>
    internal const string FileSegment = "file";

    /// <summary>The symbol segment token.</summary>
    internal const string SymbolSegment = "symbol";

    /// <summary>The memory segment token.</summary>
    internal const string MemorySegment = "mem";

    /// <summary>The vector-metadata segment token.</summary>
    internal const string VectorSegment = "vec";

    /// <summary>The vector-payload segment token.</summary>
    internal const string VectorPayloadSegment = "vpay";

    /// <summary>The vector-membership segment token.</summary>
    internal const string VectorMembershipSegment = "vmem";

    private const char Separator = '/';

    /// <summary>Builds the key for a repository root node: <c>repo/{repoId}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string Repo(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return $"{RepoSegment}{Separator}{EncodeComponent(repoId)}";
    }

    /// <summary>
    /// The ordered-range scan prefix covering every repository root marker and
    /// subtree: <c>repo/</c>. A moving-cursor scan over this range visits each
    /// registered repository exactly once (the bare <c>repo/{repoId}</c> marker
    /// sorts before that repository's <c>repo/{repoId}/...</c> subtree).
    /// </summary>
    internal static string AllReposPrefix() => $"{RepoSegment}{Separator}";

    /// <summary>
    /// Builds the ordered-range scan prefix for everything under a repository:
    /// <c>repo/{repoId}/</c>.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string RepoScanPrefix(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return $"{RepoSegment}{Separator}{EncodeComponent(repoId)}{Separator}";
    }

    /// <summary>Builds the key for a package node: <c>repo/{repoId}/pkg/{path}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="packagePath">The package path. Must not be <see langword="null"/>.</param>
    internal static string Package(string repoId, string packagePath)
    {
        ArgumentNullException.ThrowIfNull(packagePath);
        return $"{RepoScanPrefix(repoId)}{PackageSegment}{Separator}{EncodePath(packagePath)}";
    }

    /// <summary>Builds the key for a file node: <c>repo/{repoId}/file/{path}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="path">The file path relative to the repository root. Must not be <see langword="null"/>.</param>
    internal static string File(string repoId, string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return $"{RepoScanPrefix(repoId)}{FileSegment}{Separator}{EncodePath(path)}";
    }

    /// <summary>Builds the key for a symbol record: <c>repo/{repoId}/symbol/{fqName}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="fullyQualifiedName">The fully-qualified symbol name. Must not be <see langword="null"/>.</param>
    internal static string Symbol(string repoId, string fullyQualifiedName)
    {
        ArgumentNullException.ThrowIfNull(fullyQualifiedName);
        return $"{RepoScanPrefix(repoId)}{SymbolSegment}{Separator}{EncodeComponent(fullyQualifiedName)}";
    }

    /// <summary>Builds the key for a memory record: <c>repo/{repoId}/mem/{topic}/{id}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="topic">The memory topic bucket. Must not be <see langword="null"/>.</param>
    /// <param name="id">The per-topic record identifier. Must not be <see langword="null"/>.</param>
    internal static string Memory(string repoId, string topic, string id)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(id);
        return $"{RepoScanPrefix(repoId)}{MemorySegment}{Separator}{EncodeComponent(topic)}{Separator}{EncodeComponent(id)}";
    }

    /// <summary>Builds the key for a vector metadata record: <c>repo/{repoId}/vec/{vectorId}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="vectorId">The per-repository vector identifier. Must not be <see langword="null"/>.</param>
    internal static string Vector(string repoId, string vectorId)
    {
        ArgumentNullException.ThrowIfNull(vectorId);
        return $"{RepoScanPrefix(repoId)}{VectorSegment}{Separator}{EncodeComponent(vectorId)}";
    }

    /// <summary>Builds the key for a content-addressed vector payload: <c>repo/{repoId}/vpay/{contentAddress}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="contentAddress">The payload content address. Must not be <see langword="null"/>.</param>
    internal static string VectorPayload(string repoId, string contentAddress)
    {
        ArgumentNullException.ThrowIfNull(contentAddress);
        return $"{RepoScanPrefix(repoId)}{VectorPayloadSegment}{Separator}{EncodeComponent(contentAddress)}";
    }

    /// <summary>Builds the key for a vector membership record: <c>repo/{repoId}/vmem/{collection}</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="collection">The vector collection name. Must not be <see langword="null"/>.</param>
    internal static string VectorMembership(string repoId, string collection)
    {
        ArgumentNullException.ThrowIfNull(collection);
        return $"{RepoScanPrefix(repoId)}{VectorMembershipSegment}{Separator}{EncodeComponent(collection)}";
    }

    /// <summary>Builds the range-scan prefix for all file nodes in a repository: <c>repo/{repoId}/file/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string FilesPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{FileSegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all package nodes in a repository: <c>repo/{repoId}/pkg/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string PackagesPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{PackageSegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all symbol records in a repository: <c>repo/{repoId}/symbol/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string SymbolsPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{SymbolSegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all memory records in a repository: <c>repo/{repoId}/mem/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string MemoryPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{MemorySegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all vector metadata records in a repository: <c>repo/{repoId}/vec/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string VectorsPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{VectorSegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all vector payloads in a repository: <c>repo/{repoId}/vpay/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string VectorPayloadsPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{VectorPayloadSegment}{Separator}";

    /// <summary>Builds the range-scan prefix for all vector membership records in a repository: <c>repo/{repoId}/vmem/</c>.</summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    internal static string VectorMembershipsPrefix(string repoId) =>
        $"{RepoScanPrefix(repoId)}{VectorMembershipSegment}{Separator}";

    /// <summary>
    /// Builds the range-scan prefix for all memory records under a topic:
    /// <c>repo/{repoId}/mem/{topic}/</c>.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="topic">The memory topic bucket. Must not be <see langword="null"/>.</param>
    internal static string MemoryTopicPrefix(string repoId, string topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        return $"{MemoryPrefix(repoId)}{EncodeComponent(topic)}{Separator}";
    }

    /// <summary>
    /// Builds the range-scan prefix for all file nodes under a directory:
    /// <c>repo/{repoId}/file/{directory}/</c>. A trailing separator on
    /// <paramref name="directory"/> is normalised away before the single trailing
    /// separator is appended.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="directory">The directory path. Must not be <see langword="null"/>.</param>
    internal static string FilesUnderPrefix(string repoId, string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        var normalised = directory.TrimEnd(Separator);
        return normalised.Length == 0
            ? FilesPrefix(repoId)
            : $"{FilesPrefix(repoId)}{EncodePath(normalised)}{Separator}";
    }

    /// <summary>
    /// Parses a repository-context key back into its components. Returns
    /// <see langword="false"/> (with <paramref name="result"/> set to the default)
    /// for any string that is not a well-formed key.
    /// </summary>
    /// <param name="key">The key to parse.</param>
    /// <param name="result">The parsed components when parsing succeeds.</param>
    internal static bool TryParse(string key, out RepoContextKey result)
    {
        result = default;
        if (string.IsNullOrEmpty(key))
        {
            return false;
        }

        var rootPrefix = $"{RepoSegment}{Separator}";
        if (!key.StartsWith(rootPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var repoStart = rootPrefix.Length;
        var repoEnd = key.IndexOf(Separator, repoStart);
        if (repoEnd < 0)
        {
            // repo/{repoId}
            var repoOnly = DecodeComponent(key[repoStart..]);
            if (repoOnly.Length == 0)
            {
                return false;
            }

            result = new RepoContextKey { Kind = RepoContextRecordKind.Repo, RepoId = repoOnly };
            return true;
        }

        var repoId = DecodeComponent(key[repoStart..repoEnd]);
        if (repoId.Length == 0)
        {
            return false;
        }

        var segmentStart = repoEnd + 1;
        var segmentEnd = key.IndexOf(Separator, segmentStart);
        if (segmentEnd < 0)
        {
            return false;
        }

        var segment = key[segmentStart..segmentEnd];
        var payload = key[(segmentEnd + 1)..];
        if (payload.Length == 0)
        {
            return false;
        }

        switch (segment)
        {
            case FileSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.File,
                    RepoId = repoId,
                    Path = DecodePath(payload),
                };
                return true;

            case PackageSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.Package,
                    RepoId = repoId,
                    Path = DecodePath(payload),
                };
                return true;

            case SymbolSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.Symbol,
                    RepoId = repoId,
                    FullyQualifiedName = DecodeComponent(payload),
                };
                return true;

            case VectorSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.VectorMetadata,
                    RepoId = repoId,
                    VectorId = DecodeComponent(payload),
                };
                return true;

            case VectorPayloadSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.VectorPayload,
                    RepoId = repoId,
                    ContentAddress = DecodeComponent(payload),
                };
                return true;

            case VectorMembershipSegment:
                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.VectorMembership,
                    RepoId = repoId,
                    Collection = DecodeComponent(payload),
                };
                return true;

            case MemorySegment:
                var topicEnd = payload.IndexOf(Separator);
                if (topicEnd < 0)
                {
                    return false;
                }

                // A memory id is an opaque component and never contains an
                // unescaped separator, so exactly one separator may appear.
                if (payload.IndexOf(Separator, topicEnd + 1) >= 0)
                {
                    return false;
                }

                var topic = DecodeComponent(payload[..topicEnd]);
                var id = DecodeComponent(payload[(topicEnd + 1)..]);
                if (topic.Length == 0 || id.Length == 0)
                {
                    return false;
                }

                result = new RepoContextKey
                {
                    Kind = RepoContextRecordKind.Memory,
                    RepoId = repoId,
                    Topic = topic,
                    Id = id,
                };
                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Percent-encodes an opaque single component: both <c>'%'</c> and the
    /// segment separator <c>'/'</c> are escaped so the value can never introduce a
    /// stray segment boundary.
    /// </summary>
    /// <param name="value">The component to encode. Must not be <see langword="null"/>.</param>
    internal static string EncodeComponent(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return PercentEncode(value, escapeSeparator: true);
    }

    /// <summary>
    /// Percent-encodes a hierarchical path component: only <c>'%'</c> is escaped;
    /// the separator <c>'/'</c> is preserved so a directory subtree stays
    /// contiguous under an ordered range scan.
    /// </summary>
    /// <param name="value">The path to encode. Must not be <see langword="null"/>.</param>
    internal static string EncodePath(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return PercentEncode(value, escapeSeparator: false);
    }

    /// <summary>Reverses <see cref="EncodeComponent(string)"/>.</summary>
    /// <param name="value">The encoded component. Must not be <see langword="null"/>.</param>
    internal static string DecodeComponent(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return PercentDecode(value);
    }

    /// <summary>Reverses <see cref="EncodePath(string)"/>.</summary>
    /// <param name="value">The encoded path. Must not be <see langword="null"/>.</param>
    internal static string DecodePath(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return PercentDecode(value);
    }

    private static string PercentEncode(string value, bool escapeSeparator)
    {
        var needsEncoding = false;
        foreach (var c in value)
        {
            if (c == '%' || (escapeSeparator && c == Separator))
            {
                needsEncoding = true;
                break;
            }
        }

        if (!needsEncoding)
        {
            return value;
        }

        var builder = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            if (c == '%' || (escapeSeparator && c == Separator))
            {
                builder.Append('%').Append(((int)c).ToString("X2", CultureInfo.InvariantCulture));
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }

    private static string PercentDecode(string value)
    {
        if (value.IndexOf('%') < 0)
        {
            return value;
        }

        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (c == '%'
                && i + 2 < value.Length
                && TryParseHex(value[i + 1], out var high)
                && TryParseHex(value[i + 2], out var low))
            {
                builder.Append((char)((high << 4) | low));
                i += 2;
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }

    private static bool TryParseHex(char c, out int value)
    {
        switch (c)
        {
            case >= '0' and <= '9':
                value = c - '0';
                return true;
            case >= 'A' and <= 'F':
                value = c - 'A' + 10;
                return true;
            case >= 'a' and <= 'f':
                value = c - 'a' + 10;
                return true;
            default:
                value = 0;
                return false;
        }
    }
}
