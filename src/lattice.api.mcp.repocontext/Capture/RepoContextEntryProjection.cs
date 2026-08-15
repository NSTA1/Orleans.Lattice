using System.Text;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Projects a stored repository-context record's value bytes into the flat,
/// agent-readable <see cref="RepoContextEntryView"/>. Each record family is
/// deserialized to its concrete type (recovered from the key grammar) and its
/// last-writer-wins scalars, add-wins tag set, and - for memory - link relations
/// are read out through the record model's own accessors and
/// <see cref="RepoContextValues"/> helpers.
/// <para>
/// This is a pure read-side adapter: it decodes and presents existing records and
/// never writes, so it introduces no storage behaviour of its own.
/// </para>
/// </summary>
internal static class RepoContextEntryProjection
{
    /// <summary>
    /// Projects the live value at <paramref name="key"/> into a view. When
    /// <paramref name="value"/> is <see langword="null"/> the key has no live
    /// entry and an <see cref="RepoContextEntryView.Exists"/>-false view is
    /// returned carrying only the parsed identity.
    /// </summary>
    /// <param name="key">The parsed key components.</param>
    /// <param name="value">The stored value bytes, or <see langword="null"/> when absent/expired.</param>
    /// <param name="serializer">The Orleans serializer used to decode the record. Must not be <see langword="null"/>.</param>
    /// <param name="life">The entry's projected remaining life.</param>
    /// <returns>The flattened entry view.</returns>
    internal static RepoContextEntryView Project(
        RepoContextKey key,
        byte[]? value,
        Serializer serializer,
        RepoContextRemainingLife life)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        var tags = new List<string>();
        var links = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

        if (value is not null)
        {
            switch (key.Kind)
            {
                case RepoContextRecordKind.Repo:
                    ProjectRepo(serializer.Deserialize<RepoNode>(value), fields, tags);
                    break;
                case RepoContextRecordKind.Package:
                    ProjectPackage(serializer.Deserialize<PackageNode>(value), fields, tags);
                    break;
                case RepoContextRecordKind.File:
                    ProjectFile(serializer.Deserialize<FileNode>(value), fields, tags);
                    break;
                case RepoContextRecordKind.Symbol:
                    ProjectSymbol(serializer.Deserialize<SymbolRecord>(value), fields, tags);
                    break;
                case RepoContextRecordKind.Memory:
                    ProjectMemory(serializer.Deserialize<MemoryRecord>(value), fields, tags, links);
                    break;
                default:
                    break;
            }
        }

        return new RepoContextEntryView
        {
            Key = RebuildKey(key),
            Exists = value is not null,
            Kind = key.Kind.ToString(),
            RepoId = key.RepoId,
            Path = key.Path,
            FullyQualifiedName = key.FullyQualifiedName,
            Topic = key.Topic,
            Id = key.Id,
            Fields = fields,
            Tags = tags,
            Links = links,
            Expires = life.Expires,
            ExpiresAtUtc = life.ExpiresAtUtc?.ToString("O"),
            RemainingSeconds = life.Expires ? life.Remaining.TotalSeconds : null,
            HasExpired = life.HasExpired,
        };
    }

    private static string RebuildKey(RepoContextKey key) => key.Kind switch
    {
        RepoContextRecordKind.Repo => RepoContextKeys.Repo(key.RepoId),
        RepoContextRecordKind.Package => RepoContextKeys.Package(key.RepoId, key.Path!),
        RepoContextRecordKind.File => RepoContextKeys.File(key.RepoId, key.Path!),
        RepoContextRecordKind.Symbol => RepoContextKeys.Symbol(key.RepoId, key.FullyQualifiedName!),
        RepoContextRecordKind.Memory => RepoContextKeys.Memory(key.RepoId, key.Topic!, key.Id!),
        RepoContextRecordKind.VectorMetadata => RepoContextKeys.Vector(key.RepoId, key.VectorId!),
        RepoContextRecordKind.VectorPayload => RepoContextKeys.VectorPayload(key.RepoId, key.ContentAddress!),
        RepoContextRecordKind.VectorMembership => RepoContextKeys.VectorMembership(key.RepoId, key.Collection!),
        _ => RepoContextKeys.Repo(key.RepoId),
    };

    private static void ProjectRepo(RepoNode node, Dictionary<string, string> fields, List<string> tags)
    {
        AddString(fields, "displayName", node.DisplayName);
        AddString(fields, "defaultBranch", node.DefaultBranch);
        AddString(fields, "lastIngested", node.LastIngested);
        AddTags(tags, node.Tags);
    }

    private static void ProjectPackage(PackageNode node, Dictionary<string, string> fields, List<string> tags)
    {
        AddString(fields, "language", node.Language);
        AddString(fields, "version", node.Version);
        AddString(fields, "lastIngested", node.LastIngested);
        AddTags(tags, node.Tags);
    }

    private static void ProjectFile(FileNode node, Dictionary<string, string> fields, List<string> tags)
    {
        AddString(fields, "digest", node.Digest);
        AddString(fields, "language", node.Language);
        AddInt(fields, "sizeBytes", node.SizeBytes);
        AddString(fields, "lastIngested", node.LastIngested);
        AddTags(tags, node.Tags);
    }

    private static void ProjectSymbol(SymbolRecord node, Dictionary<string, string> fields, List<string> tags)
    {
        fields["kind"] = node.Kind.ToString();
        AddString(fields, "filePath", node.FilePath);
        AddInt(fields, "startLine", node.StartLine);
        AddInt(fields, "endLine", node.EndLine);
        AddString(fields, "signature", node.Signature);
        AddString(fields, "digest", node.Digest);
        AddTags(tags, node.Tags);
        var references = ReadElements(node.References);
        if (references.Count != 0)
        {
            fields["references"] = string.Join(",", references);
        }
    }

    private static void ProjectMemory(
        MemoryRecord node,
        Dictionary<string, string> fields,
        List<string> tags,
        Dictionary<string, IReadOnlyList<string>> links)
    {
        fields["kind"] = node.Kind.ToString();
        AddString(fields, "title", node.Title);
        AddString(fields, "body", node.Body);
        AddString(fields, "author", node.Author);
        AddString(fields, "provenance", node.Provenance);
        AddInt(fields, "createdAt", node.CreatedAt);
        AddTags(tags, node.Tags);

        foreach (var relation in node.Links.Adds.Keys)
        {
            var targets = node.Links.Get(relation);
            if (targets is null)
            {
                continue;
            }

            var members = ReadElements(targets);
            if (members.Count != 0)
            {
                links[relation] = members;
            }
        }
    }

    private static void AddString(Dictionary<string, string> fields, string name, BoundedRegister register)
    {
        var text = RepoContextValues.ReadString(register);
        if (text is not null)
        {
            fields[name] = text;
        }
    }

    private static void AddInt(Dictionary<string, string> fields, string name, BoundedRegister register)
    {
        var number = RepoContextValues.ReadInt64(register);
        if (number is not null)
        {
            fields[name] = number.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    private static void AddTags(List<string> tags, OrSet set) => tags.AddRange(ReadElements(set));

    private static List<string> ReadElements(OrSet set)
    {
        var elements = new List<string>();
        foreach (var element in set.Elements())
        {
            elements.Add(Encoding.UTF8.GetString(element));
        }

        elements.Sort(StringComparer.Ordinal);
        return elements;
    }
}
