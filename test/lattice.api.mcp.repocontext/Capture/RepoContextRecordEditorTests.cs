using System.Text;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for <see cref="RepoContextRecordEditor"/>: it patches scalar fields
/// and tags on every patchable record family through the record model's CRDT
/// merge (never a blind overwrite), rejects unknown fields and non-integer values
/// for integer fields with a caller-facing <see cref="McpException"/>, and yields
/// deterministic, order-independent convergence for concurrent-style patches.
/// </summary>
[TestFixture]
public sealed class RepoContextRecordEditorTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static RepoContextKey Parse(string key)
    {
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True, key);
        return parsed;
    }

    private static IReadOnlyList<string> DecodeTags(OrSet set)
        => set.Elements().Select(e => Encoding.UTF8.GetString(e)).OrderBy(s => s, StringComparer.Ordinal).ToList();

    [Test]
    public void Patch_sets_a_file_scalar_field_through_merge()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var existing = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        var result = RepoContextRecordEditor.Patch(
            key, existing,
            new Dictionary<string, string> { ["language"] = "csharp", ["digest"] = "abc" },
            addTags: null, removeTags: null, addLinks: null, removeLinks: null, Clock(100), Serializer);

        var merged = Serializer.Deserialize<FileNode>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.FieldsUpdated, Is.EqualTo(2));
            Assert.That(RepoContextValues.ReadString(merged.Language), Is.EqualTo("csharp"));
            Assert.That(RepoContextValues.ReadString(merged.Digest), Is.EqualTo("abc"));
        });
    }

    [Test]
    public void Patch_parses_an_integer_field()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var existing = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        var result = RepoContextRecordEditor.Patch(
            key, existing,
            new Dictionary<string, string> { ["sizeBytes"] = "4096" },
            addTags: null, removeTags: null, addLinks: null, removeLinks: null, Clock(100), Serializer);

        var merged = Serializer.Deserialize<FileNode>(result.Merged);
        Assert.That(RepoContextValues.ReadInt64(merged.SizeBytes), Is.EqualTo(4096));
    }

    [Test]
    public void Patch_rejects_a_non_integer_value_for_an_integer_field()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var existing = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        Assert.That(
            () => RepoContextRecordEditor.Patch(
                key, existing,
                new Dictionary<string, string> { ["sizeBytes"] = "not-a-number" },
                addTags: null, removeTags: null, addLinks: null, removeLinks: null, Clock(100), Serializer),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_rejects_an_unknown_field()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "notes", "1"));
        var existing = Serializer.SerializeToArray(
            new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" });

        Assert.That(
            () => RepoContextRecordEditor.Patch(
                key, existing,
                new Dictionary<string, string> { ["nonsense"] = "x" },
                addTags: null, removeTags: null, addLinks: null, removeLinks: null, Clock(100), Serializer),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_adds_and_removes_tags()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "notes", "1"));
        var seed = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        seed.Tags.Add(Encoding.UTF8.GetBytes("stale"), "seed", 0);
        var existing = Serializer.SerializeToArray(seed);

        var result = RepoContextRecordEditor.Patch(
            key, existing, fields: null,
            addTags: new[] { "fresh", "keep" },
            removeTags: new[] { "stale" },
            addLinks: null, removeLinks: null,
            Clock(100), Serializer);

        var merged = Serializer.Deserialize<MemoryRecord>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.TagsAdded, Is.EqualTo(2));
            Assert.That(result.TagsRemoved, Is.EqualTo(1));
            Assert.That(DecodeTags(merged.Tags), Is.EqualTo(new[] { "fresh", "keep" }));
        });
    }

    [Test]
    public void Patch_rejects_a_non_patchable_key_kind()
    {
        var key = Parse(RepoContextKeys.Vector("acme", "vec-1"));
        Assert.That(
            () => RepoContextRecordEditor.Patch(
                key, Array.Empty<byte>(),
                new Dictionary<string, string> { ["x"] = "y" },
                addTags: null, removeTags: null, addLinks: null, removeLinks: null, Clock(100), Serializer),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Concurrent_style_patches_converge_regardless_of_apply_order()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var baseline = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        // Apply an early patch then a late patch: the late value wins.
        var early = RepoContextRecordEditor.Patch(
            key, baseline, new Dictionary<string, string> { ["language"] = "csharp" },
            null, null, null, null, Clock(100), Serializer);
        var forward = RepoContextRecordEditor.Patch(
            key, early.Merged, new Dictionary<string, string> { ["language"] = "fsharp" },
            null, null, null, null, Clock(200), Serializer);

        // Apply them in the reverse order: the late (200) value still wins.
        var late = RepoContextRecordEditor.Patch(
            key, baseline, new Dictionary<string, string> { ["language"] = "fsharp" },
            null, null, null, null, Clock(200), Serializer);
        var backward = RepoContextRecordEditor.Patch(
            key, late.Merged, new Dictionary<string, string> { ["language"] = "csharp" },
            null, null, null, null, Clock(100), Serializer);

        var forwardLanguage = RepoContextValues.ReadString(Serializer.Deserialize<FileNode>(forward.Merged).Language);
        var backwardLanguage = RepoContextValues.ReadString(Serializer.Deserialize<FileNode>(backward.Merged).Language);
        Assert.Multiple(() =>
        {
            Assert.That(forwardLanguage, Is.EqualTo("fsharp"));
            Assert.That(backwardLanguage, Is.EqualTo("fsharp"),
                "A last-writer-wins field converges to the highest-clock value regardless of apply order.");
        });
    }

    [Test]
    public void ApplyTags_ignores_empty_tags_and_reports_zero_for_a_missing_removal()
    {
        var set = new OrSet();
        var (added, removed) = RepoContextRecordEditor.ApplyTags(
            set, addTags: new[] { "", "real" }, removeTags: new[] { "absent" });

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.EqualTo(1));
            Assert.That(removed, Is.EqualTo(0));
            Assert.That(DecodeTags(set), Is.EqualTo(new[] { "real" }));
        });
    }

    private static IReadOnlyDictionary<string, IReadOnlyList<string>> DecodeLinks(OrMap<string, OrSet> links)
    {
        var decoded = new Dictionary<string, IReadOnlyList<string>>();
        foreach (var relation in links.Keys())
        {
            var members = (links.Get(relation) ?? new OrSet())
                .Elements()
                .Select(e => Encoding.UTF8.GetString(e))
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
            if (members.Count != 0)
            {
                decoded[relation] = members;
            }
        }

        return decoded;
    }

    [Test]
    public void ApplyLinks_adds_and_removes_edges_under_a_relation()
    {
        var links = new OrMap<string, OrSet>();
        var broader = RepoContextKeys.Memory("acme", "glossary", "tree");
        var related = RepoContextKeys.Memory("acme", "glossary", "wal");
        links.Set("related", "seed", Seeded(related));

        var (added, removed) = RepoContextRecordEditor.ApplyLinks(
            links,
            addLinks: new Dictionary<string, IReadOnlyList<string>> { ["broader"] = new[] { broader } },
            removeLinks: new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { related } });

        var decoded = DecodeLinks(links);
        Assert.Multiple(() =>
        {
            Assert.That(added, Is.EqualTo(1));
            Assert.That(removed, Is.EqualTo(1));
            Assert.That(decoded.ContainsKey("broader"), Is.True);
            Assert.That(decoded["broader"], Is.EqualTo(new[] { broader }));
            Assert.That(decoded.ContainsKey("related"), Is.False, "The only target under 'related' was removed.");
        });
    }

    [Test]
    public void ApplyLinks_rejects_a_malformed_target_before_mutating()
    {
        var links = new OrMap<string, OrSet>();
        Assert.That(
            () => RepoContextRecordEditor.ApplyLinks(
                links,
                addLinks: new Dictionary<string, IReadOnlyList<string>> { ["broader"] = new[] { "not a key" } },
                removeLinks: null),
            Throws.InstanceOf<McpException>());
        Assert.That(links.Keys(), Is.Empty, "A rejected patch leaves the link map untouched.");
    }

    [Test]
    public void ApplyLinks_rejects_an_empty_relation_name()
    {
        var links = new OrMap<string, OrSet>();
        Assert.That(
            () => RepoContextRecordEditor.ApplyLinks(
                links,
                addLinks: new Dictionary<string, IReadOnlyList<string>>
                {
                    ["  "] = new[] { RepoContextKeys.Memory("acme", "g", "x") },
                },
                removeLinks: null),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Patch_applies_links_to_a_memory_record()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "glossary", "tree"));
        var target = RepoContextKeys.Memory("acme", "glossary", "shard");
        var existing = Serializer.SerializeToArray(
            new MemoryRecord { RepoId = "acme", Topic = "glossary", Id = "tree" });

        var result = RepoContextRecordEditor.Patch(
            key, existing, fields: null, addTags: null, removeTags: null,
            addLinks: new Dictionary<string, IReadOnlyList<string>> { ["narrower"] = new[] { target } },
            removeLinks: null, Clock(100), Serializer);

        var merged = Serializer.Deserialize<MemoryRecord>(result.Merged);
        Assert.Multiple(() =>
        {
            Assert.That(result.LinksAdded, Is.EqualTo(1));
            Assert.That(result.LinksRemoved, Is.EqualTo(0));
            Assert.That(DecodeLinks(merged.Links)["narrower"], Is.EqualTo(new[] { target }));
        });
    }

    [Test]
    public void Patch_rejects_links_on_a_non_memory_record()
    {
        var key = Parse(RepoContextKeys.File("acme", "a.cs"));
        var existing = Serializer.SerializeToArray(new FileNode { RepoId = "acme", Path = "a.cs" });

        Assert.That(
            () => RepoContextRecordEditor.Patch(
                key, existing, fields: null, addTags: null, removeTags: null,
                addLinks: new Dictionary<string, IReadOnlyList<string>>
                {
                    ["broader"] = new[] { RepoContextKeys.Memory("acme", "g", "x") },
                },
                removeLinks: null, Clock(100), Serializer),
            Throws.InstanceOf<McpException>());
    }

    [Test]
    public void Concurrent_style_link_patches_converge_regardless_of_apply_order()
    {
        var left = new OrMap<string, OrSet>();
        var right = new OrMap<string, OrSet>();
        var a = RepoContextKeys.Memory("acme", "g", "a");
        var b = RepoContextKeys.Memory("acme", "g", "b");

        RepoContextRecordEditor.ApplyLinks(
            left, new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { a } }, null);
        RepoContextRecordEditor.ApplyLinks(
            right, new Dictionary<string, IReadOnlyList<string>> { ["related"] = new[] { b } }, null);

        var forward = DecodeLinks(OrMap<string, OrSet>.Merge(left, right));
        var backward = DecodeLinks(OrMap<string, OrSet>.Merge(right, left));

        Assert.Multiple(() =>
        {
            Assert.That(forward["related"], Is.EqualTo(new[] { a, b }));
            Assert.That(backward["related"], Is.EqualTo(new[] { a, b }),
                "Concurrent add-wins edges under the same relation both survive, order-independent.");
        });
    }

    private static OrSet Seeded(string target)
    {
        var set = new OrSet();
        set.Add(Encoding.UTF8.GetBytes(target), "seed", 0L);
        return set;
    }
}
