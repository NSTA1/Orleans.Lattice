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
            addTags: null, removeTags: null, Clock(100), Serializer);

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
            addTags: null, removeTags: null, Clock(100), Serializer);

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
                addTags: null, removeTags: null, Clock(100), Serializer),
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
                addTags: null, removeTags: null, Clock(100), Serializer),
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
                addTags: null, removeTags: null, Clock(100), Serializer),
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
            null, null, Clock(100), Serializer);
        var forward = RepoContextRecordEditor.Patch(
            key, early.Merged, new Dictionary<string, string> { ["language"] = "fsharp" },
            null, null, Clock(200), Serializer);

        // Apply them in the reverse order: the late (200) value still wins.
        var late = RepoContextRecordEditor.Patch(
            key, baseline, new Dictionary<string, string> { ["language"] = "fsharp" },
            null, null, Clock(200), Serializer);
        var backward = RepoContextRecordEditor.Patch(
            key, late.Merged, new Dictionary<string, string> { ["language"] = "csharp" },
            null, null, Clock(100), Serializer);

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
}
