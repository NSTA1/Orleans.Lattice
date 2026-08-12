using System.Globalization;
using System.Text;
using ModelContextProtocol;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Applies a scalar-field and tag patch to an existing repository-context record
/// through the record model's own CRDT join, never a blind overwrite. Each patched
/// scalar is authored as a fresh last-writer-wins register at the supplied hybrid
/// logical clock and folded into the stored record via its static <c>Merge</c>, so
/// two concurrent patches converge deterministically; tag additions and removals
/// are applied to the merged record's add-wins set.
/// <para>
/// This is the shared edit primitive behind the <c>repocontext_update</c> tool. It
/// reuses the #1429 record model and the <see cref="RepoContextValues"/> helpers
/// and adds no storage primitive of its own.
/// </para>
/// </summary>
internal static class RepoContextRecordEditor
{
    /// <summary>The outcome of a patch: the re-encoded record bytes and the counts applied.</summary>
    /// <param name="Merged">The merged record's serialized bytes, ready to write back.</param>
    /// <param name="FieldsUpdated">The number of scalar fields set to a new value.</param>
    /// <param name="TagsAdded">The number of tags added to the record's set.</param>
    /// <param name="TagsRemoved">The number of tags removed from the record's set.</param>
    internal readonly record struct PatchResult(
        byte[] Merged, int FieldsUpdated, int TagsAdded, int TagsRemoved);

    /// <summary>
    /// Patches the record decoded from <paramref name="existing"/> at
    /// <paramref name="key"/> and returns the merged bytes.
    /// </summary>
    /// <param name="key">The parsed key identifying the record and its family.</param>
    /// <param name="existing">The record's current stored bytes. Must not be <see langword="null"/>.</param>
    /// <param name="fields">The scalar field patches (field name to value), or <see langword="null"/>.</param>
    /// <param name="addTags">Tags to add, or <see langword="null"/>.</param>
    /// <param name="removeTags">Tags to remove, or <see langword="null"/>.</param>
    /// <param name="clock">The hybrid logical clock the patched registers are authored at.</param>
    /// <param name="serializer">The Orleans serializer. Must not be <see langword="null"/>.</param>
    /// <returns>The patch outcome.</returns>
    /// <exception cref="McpException">A field name is not valid for the record family, or an integer field value does not parse.</exception>
    internal static PatchResult Patch(
        RepoContextKey key,
        byte[] existing,
        IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags,
        IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock,
        Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(serializer);

        return key.Kind switch
        {
            RepoContextRecordKind.Repo => PatchRepo(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Package => PatchPackage(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.File => PatchFile(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Symbol => PatchSymbol(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Memory => PatchMemory(key, existing, fields, addTags, removeTags, clock, serializer),
            _ => throw new McpException(
                $"The key kind '{key.Kind}' is not a patchable record; only structural and memory records can be updated."),
        };
    }

    private static PatchResult PatchRepo(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock, Serializer serializer)
    {
        var current = serializer.Deserialize<RepoNode>(existing);
        var delta = new RepoNode { RepoId = key.RepoId };
        var updated = 0;
        foreach (var (name, value) in Enumerate(fields))
        {
            switch (name.ToLowerInvariant())
            {
                case "displayname": delta = delta with { DisplayName = RepoContextValues.Lww(value, clock) }; break;
                case "defaultbranch": delta = delta with { DefaultBranch = RepoContextValues.Lww(value, clock) }; break;
                case "lastingested": delta = delta with { LastIngested = RepoContextValues.Lww(value, clock) }; break;
                default: throw UnknownField(name, key.Kind);
            }

            updated++;
        }

        var merged = RepoNode.Merge(delta, current);
        var (added, removed) = ApplyTags(merged.Tags, addTags, removeTags);
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed);
    }

    private static PatchResult PatchPackage(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock, Serializer serializer)
    {
        var current = serializer.Deserialize<PackageNode>(existing);
        var delta = new PackageNode { RepoId = key.RepoId, Path = key.Path ?? string.Empty };
        var updated = 0;
        foreach (var (name, value) in Enumerate(fields))
        {
            switch (name.ToLowerInvariant())
            {
                case "language": delta = delta with { Language = RepoContextValues.Lww(value, clock) }; break;
                case "version": delta = delta with { Version = RepoContextValues.Lww(value, clock) }; break;
                case "lastingested": delta = delta with { LastIngested = RepoContextValues.Lww(value, clock) }; break;
                default: throw UnknownField(name, key.Kind);
            }

            updated++;
        }

        var merged = PackageNode.Merge(delta, current);
        var (added, removed) = ApplyTags(merged.Tags, addTags, removeTags);
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed);
    }

    private static PatchResult PatchFile(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock, Serializer serializer)
    {
        var current = serializer.Deserialize<FileNode>(existing);
        var delta = new FileNode { RepoId = key.RepoId, Path = key.Path ?? string.Empty };
        var updated = 0;
        foreach (var (name, value) in Enumerate(fields))
        {
            switch (name.ToLowerInvariant())
            {
                case "digest": delta = delta with { Digest = RepoContextValues.Lww(value, clock) }; break;
                case "language": delta = delta with { Language = RepoContextValues.Lww(value, clock) }; break;
                case "sizebytes": delta = delta with { SizeBytes = RepoContextValues.Lww(ParseInt(name, value), clock) }; break;
                case "lastingested": delta = delta with { LastIngested = RepoContextValues.Lww(value, clock) }; break;
                default: throw UnknownField(name, key.Kind);
            }

            updated++;
        }

        var merged = FileNode.Merge(delta, current);
        var (added, removed) = ApplyTags(merged.Tags, addTags, removeTags);
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed);
    }

    private static PatchResult PatchSymbol(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock, Serializer serializer)
    {
        var current = serializer.Deserialize<SymbolRecord>(existing);
        var delta = new SymbolRecord { RepoId = key.RepoId, FullyQualifiedName = key.FullyQualifiedName ?? string.Empty };
        var updated = 0;
        foreach (var (name, value) in Enumerate(fields))
        {
            switch (name.ToLowerInvariant())
            {
                case "filepath": delta = delta with { FilePath = RepoContextValues.Lww(value, clock) }; break;
                case "startline": delta = delta with { StartLine = RepoContextValues.Lww(ParseInt(name, value), clock) }; break;
                case "endline": delta = delta with { EndLine = RepoContextValues.Lww(ParseInt(name, value), clock) }; break;
                case "signature": delta = delta with { Signature = RepoContextValues.Lww(value, clock) }; break;
                case "digest": delta = delta with { Digest = RepoContextValues.Lww(value, clock) }; break;
                default: throw UnknownField(name, key.Kind);
            }

            updated++;
        }

        var merged = SymbolRecord.Merge(delta, current);
        var (added, removed) = ApplyTags(merged.Tags, addTags, removeTags);
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed);
    }

    private static PatchResult PatchMemory(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        HybridLogicalClock clock, Serializer serializer)
    {
        var current = serializer.Deserialize<MemoryRecord>(existing);
        var delta = new MemoryRecord { RepoId = key.RepoId, Topic = key.Topic ?? string.Empty, Id = key.Id ?? string.Empty };
        var updated = 0;
        foreach (var (name, value) in Enumerate(fields))
        {
            switch (name.ToLowerInvariant())
            {
                case "title": delta = delta with { Title = RepoContextValues.Lww(value, clock) }; break;
                case "body": delta = delta with { Body = RepoContextValues.Lww(value, clock) }; break;
                case "author": delta = delta with { Author = RepoContextValues.Lww(value, clock) }; break;
                case "provenance": delta = delta with { Provenance = RepoContextValues.Lww(value, clock) }; break;
                default: throw UnknownField(name, key.Kind);
            }

            updated++;
        }

        var merged = MemoryRecord.Merge(delta, current);
        var (added, removed) = ApplyTags(merged.Tags, addTags, removeTags);
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed);
    }

    /// <summary>
    /// Adds and removes tags on <paramref name="tags"/>. Each addition mints a
    /// fresh causal dot (a unique replica id) so concurrent additions of the same
    /// tag both survive; a removal tombstones the currently-observed dots.
    /// </summary>
    internal static (int Added, int Removed) ApplyTags(
        OrSet tags, IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags)
    {
        var added = 0;
        if (addTags is not null)
        {
            foreach (var tag in addTags)
            {
                if (string.IsNullOrEmpty(tag))
                {
                    continue;
                }

                tags.Add(Encoding.UTF8.GetBytes(tag), Guid.NewGuid().ToString("N"), 0L);
                added++;
            }
        }

        var removed = 0;
        if (removeTags is not null)
        {
            foreach (var tag in removeTags)
            {
                if (string.IsNullOrEmpty(tag))
                {
                    continue;
                }

                if (tags.Remove(Encoding.UTF8.GetBytes(tag)))
                {
                    removed++;
                }
            }
        }

        return (added, removed);
    }

    private static IEnumerable<KeyValuePair<string, string>> Enumerate(
        IReadOnlyDictionary<string, string>? fields)
        => fields ?? Enumerable.Empty<KeyValuePair<string, string>>();

    private static long ParseInt(string name, string value)
        => long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new McpException($"The '{name}' field expects an integer value but got '{value}'.");

    private static McpException UnknownField(string name, RepoContextRecordKind kind)
        => new($"The field '{name}' is not a settable scalar on a {kind} record.");
}
