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
    /// <param name="LinksAdded">The number of knowledge-linking edges added (memory records only).</param>
    /// <param name="LinksRemoved">The number of knowledge-linking edges removed (memory records only).</param>
    internal readonly record struct PatchResult(
        byte[] Merged, int FieldsUpdated, int TagsAdded, int TagsRemoved, int LinksAdded, int LinksRemoved);

    /// <summary>
    /// Patches the record decoded from <paramref name="existing"/> at
    /// <paramref name="key"/> and returns the merged bytes.
    /// </summary>
    /// <param name="key">The parsed key identifying the record and its family.</param>
    /// <param name="existing">The record's current stored bytes. Must not be <see langword="null"/>.</param>
    /// <param name="fields">The scalar field patches (field name to value), or <see langword="null"/>.</param>
    /// <param name="addTags">Tags to add, or <see langword="null"/>.</param>
    /// <param name="removeTags">Tags to remove, or <see langword="null"/>.</param>
    /// <param name="addLinks">Knowledge-linking edges to add (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="removeLinks">Knowledge-linking edges to remove (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="capturedLinkDigests">For a memory patch, the content digest each newly-added structural link target currently carries (target key to digest), captured by the caller which has store access; or <see langword="null"/>. Ignored for non-memory records.</param>
    /// <param name="clock">The hybrid logical clock the patched registers are authored at.</param>
    /// <param name="serializer">The Orleans serializer. Must not be <see langword="null"/>.</param>
    /// <returns>The patch outcome.</returns>
    /// <exception cref="McpException">A field name is not valid for the record family, an integer field value does not parse, a link target is not a well-formed key, or links were supplied for a non-memory record.</exception>
    internal static PatchResult Patch(
        RepoContextKey key,
        byte[] existing,
        IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags,
        IReadOnlyList<string>? removeTags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        HybridLogicalClock clock,
        Serializer serializer,
        IReadOnlyDictionary<string, string>? capturedLinkDigests = null)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(serializer);

        if (key.Kind != RepoContextRecordKind.Memory && (HasLinks(addLinks) || HasLinks(removeLinks)))
        {
            throw new McpException(
                $"Knowledge-linking edges are only supported on memory records, not on a {key.Kind} record.");
        }

        return key.Kind switch
        {
            RepoContextRecordKind.Repo => PatchRepo(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Package => PatchPackage(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.File => PatchFile(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Symbol => PatchSymbol(key, existing, fields, addTags, removeTags, clock, serializer),
            RepoContextRecordKind.Memory => PatchMemory(key, existing, fields, addTags, removeTags, addLinks, removeLinks, capturedLinkDigests, clock, serializer),
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
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed, 0, 0);
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
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed, 0, 0);
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
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed, 0, 0);
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
        return new PatchResult(serializer.SerializeToArray(merged), updated, added, removed, 0, 0);
    }

    private static PatchResult PatchMemory(
        RepoContextKey key, byte[] existing, IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags, IReadOnlyList<string>? removeTags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        IReadOnlyDictionary<string, string>? capturedLinkDigests,
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
        var (tagsAdded, tagsRemoved) = ApplyTags(merged.Tags, addTags, removeTags);
        var (linksAdded, linksRemoved) = ApplyLinks(merged.Links, addLinks, removeLinks);
        ApplyLinkDigests(merged.LinkDigests, capturedLinkDigests, removeLinks, clock);
        return new PatchResult(
            serializer.SerializeToArray(merged), updated, tagsAdded, tagsRemoved, linksAdded, linksRemoved);
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

    /// <summary>
    /// Adds and removes knowledge-linking edges on <paramref name="links"/>, an
    /// observed-remove map from a relation name to an add-wins set of target keys.
    /// Each addition mints a fresh causal dot so concurrent additions of the same
    /// edge both survive; a removal tombstones the currently-observed dots for that
    /// target under the relation. Target keys are validated up front (fail closed),
    /// so a malformed target aborts the whole patch before any mutation.
    /// </summary>
    /// <param name="links">The record's link map. Must not be <see langword="null"/>.</param>
    /// <param name="addLinks">Edges to add (relation to target keys), or <see langword="null"/>.</param>
    /// <param name="removeLinks">Edges to remove (relation to target keys), or <see langword="null"/>.</param>
    /// <returns>The number of edges added and removed.</returns>
    /// <exception cref="McpException">A relation name is empty or a target is not a well-formed repository-context key.</exception>
    internal static (int Added, int Removed) ApplyLinks(
        OrMap<string, OrSet> links,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks)
    {
        ArgumentNullException.ThrowIfNull(links);

        // Validate the whole request before mutating anything so a single bad
        // target never leaves a half-applied edge set behind.
        ValidateLinks(addLinks);
        ValidateLinks(removeLinks);

        var relations = new HashSet<string>(StringComparer.Ordinal);
        if (addLinks is not null) { foreach (var relation in addLinks.Keys) { relations.Add(relation); } }
        if (removeLinks is not null) { foreach (var relation in removeLinks.Keys) { relations.Add(relation); } }

        var added = 0;
        var removed = 0;
        foreach (var relation in relations)
        {
            var current = links.Get(relation) ?? new OrSet();
            var mutated = false;

            if (addLinks is not null && addLinks.TryGetValue(relation, out var toAdd) && toAdd is not null)
            {
                foreach (var target in toAdd)
                {
                    current.Add(Encoding.UTF8.GetBytes(target), Guid.NewGuid().ToString("N"), 0L);
                    added++;
                    mutated = true;
                }
            }

            if (removeLinks is not null && removeLinks.TryGetValue(relation, out var toRemove) && toRemove is not null)
            {
                foreach (var target in toRemove)
                {
                    if (current.Remove(Encoding.UTF8.GetBytes(target)))
                    {
                        removed++;
                        mutated = true;
                    }
                }
            }

            if (mutated)
            {
                // Snapshot the mutated relation set back under a fresh map dot; the
                // record's OrMap merge folds every live snapshot's OrSet, so the
                // edit converges with concurrent edits under the same relation.
                links.Set(relation, Guid.NewGuid().ToString("N"), current);
            }
        }

        return (added, removed);
    }

    /// <summary>
    /// Records the captured content digest of each newly-linked structural target
    /// on <paramref name="linkDigests"/>, and drops the captured digest of each
    /// removed target. Each captured digest is authored as a fresh last-writer-wins
    /// register at <paramref name="clock"/> so concurrent captures converge; a
    /// removal tombstones the target's map entry. Only a captured digest is stored,
    /// so memory-to-memory edges (which the caller does not capture) leave no entry.
    /// </summary>
    /// <param name="linkDigests">The record's link-digest map. Must not be <see langword="null"/>.</param>
    /// <param name="capturedDigests">The digest each newly-added target currently carries (target key to digest), or <see langword="null"/>.</param>
    /// <param name="removeLinks">The edges being removed, whose captured digests are dropped, or <see langword="null"/>.</param>
    /// <param name="clock">The hybrid logical clock the captured digests are authored at.</param>
    internal static void ApplyLinkDigests(
        OrMap<string, BoundedRegister> linkDigests,
        IReadOnlyDictionary<string, string>? capturedDigests,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        HybridLogicalClock clock)
    {
        ArgumentNullException.ThrowIfNull(linkDigests);

        if (capturedDigests is not null)
        {
            foreach (var (target, digest) in capturedDigests)
            {
                linkDigests.Set(target, Guid.NewGuid().ToString("N"), RepoContextValues.Lww(digest, clock));
            }
        }

        if (removeLinks is not null)
        {
            foreach (var (_, targets) in removeLinks)
            {
                if (targets is null)
                {
                    continue;
                }

                foreach (var target in targets)
                {
                    linkDigests.Remove(target);
                }
            }
        }
    }

    private static bool HasLinks(IReadOnlyDictionary<string, IReadOnlyList<string>>? links)
    {
        if (links is null)
        {
            return false;
        }

        foreach (var (_, targets) in links)
        {
            if (targets is { Count: > 0 })
            {
                return true;
            }
        }

        return false;
    }

    private static void ValidateLinks(IReadOnlyDictionary<string, IReadOnlyList<string>>? links)
    {
        if (links is null)
        {
            return;
        }

        foreach (var (relation, targets) in links)
        {
            if (string.IsNullOrWhiteSpace(relation))
            {
                throw new McpException("A link relation name must be a non-empty string.");
            }

            if (targets is null)
            {
                continue;
            }

            foreach (var target in targets)
            {
                if (string.IsNullOrWhiteSpace(target) || !RepoContextKeys.TryParse(target, out _))
                {
                    throw new McpException(
                        $"The link target '{target}' under relation '{relation}' is not a well-formed "
                        + "repository-context key (expected 'repo/{repoId}/...').");
                }
            }
        }
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
