using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the CRDT-backed merge of the repository-context record model:
/// concurrent edits from different agents / sessions to the same record converge
/// without loss, and the merge is commutative and idempotent across the LWW
/// scalar, observed-remove set/map, and grow-only set families.
/// </summary>
[TestFixture]
public sealed class RepoContextRecordMergeTests
{
    private static HybridLogicalClock Clock(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static OrSet SetWith(string element, string replicaId, long counter)
    {
        var set = new OrSet();
        set.Add(Encoding.UTF8.GetBytes(element), replicaId, counter);
        return set;
    }

    private static IReadOnlyList<string> Decode(OrSet set)
        => set.Elements().Select(e => Encoding.UTF8.GetString(e)).OrderBy(s => s, StringComparer.Ordinal).ToList();

    private static IReadOnlyList<string> Decode(GSet set)
        => set.Values().Select(e => Encoding.UTF8.GetString(e)).OrderBy(s => s, StringComparer.Ordinal).ToList();

    [Test]
    public void FileNode_concurrent_scalar_and_set_edits_converge()
    {
        var baseline = new FileNode { RepoId = "acme", Path = "a.cs" };

        // Agent A: sets language early and tags "core".
        var a = baseline with
        {
            Language = RepoContextValues.Lww("csharp", Clock(100)),
            Tags = SetWith("core", "A", 1),
        };

        // Agent B: sets language later (should win) and tags "api", plus a blob.
        var blob = Encoding.UTF8.GetBytes("snippet-1");
        var bContent = new GSet();
        bContent.Add(blob);
        var b = baseline with
        {
            Language = RepoContextValues.Lww("fsharp", Clock(200)),
            Tags = SetWith("api", "B", 1),
            ContentBlobs = bContent,
        };

        var merged = FileNode.Merge(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(merged.RepoId, Is.EqualTo("acme"));
            Assert.That(merged.Path, Is.EqualTo("a.cs"));
            Assert.That(RepoContextValues.ReadString(merged.Language), Is.EqualTo("fsharp"));
            Assert.That(Decode(merged.Tags), Is.EqualTo(new[] { "api", "core" }));
            Assert.That(Decode(merged.ContentBlobs), Is.EqualTo(new[] { "snippet-1" }));
        });
    }

    [Test]
    public void FileNode_merge_is_commutative_over_observable_state()
    {
        var a = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            Digest = RepoContextValues.Lww("d1", Clock(100)),
            Tags = SetWith("x", "A", 1),
        };
        var b = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            Digest = RepoContextValues.Lww("d2", Clock(200)),
            Tags = SetWith("y", "B", 1),
        };

        var forward = FileNode.Merge(a, b);
        var backward = FileNode.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(forward.Digest),
                Is.EqualTo(RepoContextValues.ReadString(backward.Digest)));
            Assert.That(Decode(forward.Tags), Is.EqualTo(Decode(backward.Tags)));
        });
    }

    [Test]
    public void FileNode_merge_is_idempotent()
    {
        var node = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            Digest = RepoContextValues.Lww("d1", Clock(100)),
            Tags = SetWith("x", "A", 1),
        };

        var once = FileNode.Merge(node, node);
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(once.Digest), Is.EqualTo("d1"));
            Assert.That(Decode(once.Tags), Is.EqualTo(new[] { "x" }));
        });
    }

    [Test]
    public void SymbolRecord_preserves_kind_and_merges_references()
    {
        var baseline = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = "Acme.Program.Main",
            Kind = SymbolKind.Method,
        };

        var a = baseline with
        {
            Signature = RepoContextValues.Lww("void Main()", Clock(100)),
            References = SetWith("System.Console.WriteLine", "A", 1),
        };
        var b = baseline with
        {
            Signature = RepoContextValues.Lww("int Main(string[])", Clock(200)),
            References = SetWith("System.Environment.Exit", "B", 1),
        };

        var merged = SymbolRecord.Merge(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Kind, Is.EqualTo(SymbolKind.Method));
            Assert.That(RepoContextValues.ReadString(merged.Signature), Is.EqualTo("int Main(string[])"));
            Assert.That(Decode(merged.References),
                Is.EqualTo(new[] { "System.Console.WriteLine", "System.Environment.Exit" }));
        });
    }

    [Test]
    public void SymbolRecord_recovers_kind_from_the_other_replica_when_one_is_unspecified()
    {
        var known = new SymbolRecord { RepoId = "acme", FullyQualifiedName = "X", Kind = SymbolKind.Type };
        var unknown = new SymbolRecord { RepoId = "acme", FullyQualifiedName = "X" };

        Assert.Multiple(() =>
        {
            Assert.That(SymbolRecord.Merge(known, unknown).Kind, Is.EqualTo(SymbolKind.Type));
            Assert.That(SymbolRecord.Merge(unknown, known).Kind, Is.EqualTo(SymbolKind.Type));
        });
    }

    [Test]
    public void MemoryRecord_concurrent_edits_converge_across_every_family()
    {
        var baseline = new MemoryRecord
        {
            RepoId = "acme",
            Topic = "decisions",
            Id = "0001",
            Kind = MemoryKind.Decision,
        };

        var aLinks = new OrMap<string, OrSet>();
        aLinks.Set("supersedes", "A", SetWith("repo/acme/mem/decisions/0000", "A", 1));
        var aRevisions = new GSet();
        aRevisions.Add(Encoding.UTF8.GetBytes("rev-A"));
        var a = baseline with
        {
            Body = RepoContextValues.Lww("first body", Clock(100)),
            Tags = SetWith("architecture", "A", 1),
            Links = aLinks,
            Revisions = aRevisions,
        };

        var bLinks = new OrMap<string, OrSet>();
        bLinks.Set("relates-to", "B", SetWith("repo/acme/file/a.cs", "B", 1));
        var bRevisions = new GSet();
        bRevisions.Add(Encoding.UTF8.GetBytes("rev-B"));
        var b = baseline with
        {
            Body = RepoContextValues.Lww("second body", Clock(200)),
            Tags = SetWith("db", "B", 1),
            Links = bLinks,
            Revisions = bRevisions,
        };

        var merged = MemoryRecord.Merge(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Kind, Is.EqualTo(MemoryKind.Decision));
            Assert.That(RepoContextValues.ReadString(merged.Body), Is.EqualTo("second body"));
            Assert.That(Decode(merged.Tags), Is.EqualTo(new[] { "architecture", "db" }));
            Assert.That(merged.Links.ContainsKey("supersedes"), Is.True);
            Assert.That(merged.Links.ContainsKey("relates-to"), Is.True);
            Assert.That(Decode(merged.Revisions), Is.EqualTo(new[] { "rev-A", "rev-B" }));
        });
    }

    [Test]
    public void MemoryRecord_concurrent_links_under_the_same_relation_both_survive()
    {
        var baseline = new MemoryRecord { RepoId = "acme", Topic = "t", Id = "1" };

        var aLinks = new OrMap<string, OrSet>();
        aLinks.Set("relates-to", "A", SetWith("target-a", "A", 1));
        var a = baseline with { Links = aLinks };

        var bLinks = new OrMap<string, OrSet>();
        bLinks.Set("relates-to", "B", SetWith("target-b", "B", 1));
        var b = baseline with { Links = bLinks };

        var merged = MemoryRecord.Merge(a, b);
        var relation = merged.Links.Get("relates-to");

        Assert.That(relation, Is.Not.Null);
        Assert.That(Decode(relation!), Is.EqualTo(new[] { "target-a", "target-b" }));
    }

    [Test]
    public void MemoryRecord_merge_folds_link_digests_last_writer_wins()
    {
        var baseline = new MemoryRecord { RepoId = "acme", Topic = "t", Id = "1" };
        var target = RepoContextKeys.File("acme", "src/A.cs");

        var aDigests = new OrMap<string, BoundedRegister>();
        aDigests.Set(target, "A", RepoContextValues.Lww("old", Clock(100)));
        var a = baseline with { LinkDigests = aDigests };

        var bDigests = new OrMap<string, BoundedRegister>();
        bDigests.Set(target, "B", RepoContextValues.Lww("new", Clock(200)));
        var b = baseline with { LinkDigests = bDigests };

        var forward = MemoryRecord.Merge(a, b);
        var backward = MemoryRecord.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(forward.LinkDigests.Get(target)!), Is.EqualTo("new"));
            Assert.That(RepoContextValues.ReadString(backward.LinkDigests.Get(target)!), Is.EqualTo("new"),
                "The higher-clock captured digest wins regardless of merge order.");
        });
    }

    [Test]
    public void RepoNode_and_PackageNode_merge_scalars_and_tags()
    {
        var repoMerged = RepoNode.Merge(
            new RepoNode { RepoId = "acme", DisplayName = RepoContextValues.Lww("Old", Clock(100)), Tags = SetWith("p", "A", 1) },
            new RepoNode { RepoId = "acme", DisplayName = RepoContextValues.Lww("New", Clock(200)), Tags = SetWith("q", "B", 1) });

        var packageMerged = PackageNode.Merge(
            new PackageNode { RepoId = "acme", Path = "src", Version = RepoContextValues.Lww("1.0", Clock(100)) },
            new PackageNode { RepoId = "acme", Path = "src", Version = RepoContextValues.Lww("2.0", Clock(200)) });

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(repoMerged.DisplayName), Is.EqualTo("New"));
            Assert.That(Decode(repoMerged.Tags), Is.EqualTo(new[] { "p", "q" }));
            Assert.That(RepoContextValues.ReadString(packageMerged.Version), Is.EqualTo("2.0"));
        });
    }

    [Test]
    public void Merge_rejects_null_arguments()
    {
        var node = new FileNode { RepoId = "acme", Path = "a.cs" };
        Assert.Multiple(() =>
        {
            Assert.That(() => FileNode.Merge(null!, node), Throws.ArgumentNullException);
            Assert.That(() => FileNode.Merge(node, null!), Throws.ArgumentNullException);
            Assert.That(() => SymbolRecord.Merge(null!, new SymbolRecord()), Throws.ArgumentNullException);
            Assert.That(() => MemoryRecord.Merge(null!, new MemoryRecord()), Throws.ArgumentNullException);
            Assert.That(() => RepoNode.Merge(null!, new RepoNode()), Throws.ArgumentNullException);
            Assert.That(() => PackageNode.Merge(null!, new PackageNode()), Throws.ArgumentNullException);
        });
    }
}
