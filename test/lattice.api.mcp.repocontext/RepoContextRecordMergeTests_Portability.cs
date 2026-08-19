using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Fast unit tests for the value-level portability helpers that need no cluster:
/// the default CRDT merge strategy folds two serialized record states through the
/// record model's join (and is idempotent), and the prefix upper-bound helper
/// computes the correct exclusive scan bound.
/// </summary>
[TestFixture]
public sealed class RepoContextRecordMergeTests_Portability
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public void Default_merge_folds_two_serialized_states_through_the_record_join()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.File("acme", "a.cs");

        var existing = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            Language = RepoContextValues.Lww("csharp", Clock(100)),
        };
        var incoming = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            Language = RepoContextValues.Lww("fsharp", Clock(200)),
        };

        var mergedBytes = merge(key, _serializer.SerializeToArray(existing), _serializer.SerializeToArray(incoming));
        var merged = _serializer.Deserialize<FileNode>(mergedBytes);

        // LWW: the later clock (200) wins.
        Assert.That(RepoContextValues.ReadString(merged.Language), Is.EqualTo("fsharp"));
    }

    [Test]
    public void Default_merge_is_idempotent_for_an_identical_re_import()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Memory("acme", "notes", "n1");

        var tags = new OrSet();
        tags.Add(Encoding.UTF8.GetBytes("core"), "A", 1);
        var record = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "n1", Tags = tags };

        // A memory value is stored as the MvRegister envelope, not a bare record, so a
        // re-import folds two envelopes: merging one with itself yields the same
        // logical state (CRDT idempotency), which is exactly a re-import of the same
        // snapshot.
        var bytes = MemoryRegisterTestEncoding.EncodeSingle(_serializer, "A", record);
        var mergedBytes = merge(key, bytes, bytes);
        var folded = RepoContextMemoryCodec.Fold(mergedBytes, _serializer);

        Assert.That(folded, Is.Not.Null);
        var elements = folded!.Tags.Elements().Select(e => Encoding.UTF8.GetString(e)).ToList();
        Assert.That(elements, Is.EqualTo(new[] { "core" }));
    }

    [Test]
    public void Default_merge_of_memory_unions_two_concurrent_cross_cluster_envelopes()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var key = RepoContextKeys.Memory("acme", "notes", "n1");

        // Two clusters each imported a snapshot that captured their own concurrent
        // write. A re-import that folds the two envelopes must keep both writes' tags,
        // never collapse to one - the portability equivalent of the live read fold.
        var aTags = new OrSet();
        aTags.Add(Encoding.UTF8.GetBytes("from-a"), "A", 1);
        var bTags = new OrSet();
        bTags.Add(Encoding.UTF8.GetBytes("from-b"), "B", 1);
        var a = MemoryRegisterTestEncoding.EncodeSingle(
            _serializer, "clusterA", new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "n1", Tags = aTags });
        var b = MemoryRegisterTestEncoding.EncodeSingle(
            _serializer, "clusterB", new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "n1", Tags = bTags });

        var folded = RepoContextMemoryCodec.Fold(merge(key, a, b), _serializer);

        Assert.That(folded, Is.Not.Null);
        var elements = folded!.Tags.Elements().Select(e => Encoding.UTF8.GetString(e)).ToList();
        Assert.That(elements, Is.EquivalentTo(new[] { "from-a", "from-b" }),
            "Both concurrent cross-cluster writes survive the re-import fold.");
    }

    [Test]
    public void Default_merge_returns_incoming_when_no_existing_value()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var incoming = new byte[] { 1, 2, 3 };

        var result = merge(RepoContextKeys.File("acme", "a.cs"), null, incoming);

        Assert.That(result, Is.EqualTo(incoming));
    }

    [Test]
    public void Default_merge_returns_incoming_for_an_unparseable_key()
    {
        var merge = RepoContextRecordMerge.Default(_serializer);
        var incoming = new byte[] { 4, 5, 6 };

        var result = merge("not-a-repo-context-key", new byte[] { 7 }, incoming);

        Assert.That(result, Is.EqualTo(incoming));
    }

    [Test]
    public void PrefixUpperBound_increments_the_last_character()
    {
        Assert.That(RepoContextPortability.PrefixUpperBound("repo/acme/"), Is.EqualTo("repo/acme0"));
    }

    [Test]
    public void PrefixUpperBound_of_empty_prefix_is_null()
    {
        Assert.That(RepoContextPortability.PrefixUpperBound(string.Empty), Is.Null);
    }
}
