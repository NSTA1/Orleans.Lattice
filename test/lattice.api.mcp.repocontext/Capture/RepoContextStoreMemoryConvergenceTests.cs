using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for the multi-master agent-memory plane on
/// <see cref="RepoContextStore"/>. Memory is stored as an
/// <see cref="MvRegister"/> envelope authored through
/// <see cref="RepoContextMemoryCodec"/>, so two clusters' concurrent writes to the
/// same key both survive (each mints its own dot) and fold back through
/// <see cref="MemoryRecord.Merge(MemoryRecord, MemoryRecord)"/> on read, instead of one
/// whole record being lost to last-writer-wins. These tests prove the store's read
/// path folds a genuinely concurrent register, that memory TTL is preserved across the
/// CRDT write path, and that forget (hard and soft-lapse) is not regressed.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// The two concurrent cross-cluster writes are reproduced deterministically by applying
/// each cluster's write as an independently-minted <see cref="MvRegister"/> delta - the
/// exact path replication uses to apply a peer cluster's delta - so neither observes the
/// other and both survive the merge, with no wall-clock or ordering dependence.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreMemoryConvergenceTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice MemoryTree(RepoContextMcpHarness harness)
        => harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);

    private static Serializer Serializer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<Serializer>();

    private static MvRegister SingleWrite(Serializer serializer, string replicaId, MemoryRecord record)
    {
        var register = new MvRegister();
        register.Set(replicaId, serializer.SerializeToArray(record));
        return register;
    }

    [Test]
    public async Task Recall_folds_two_concurrent_cross_cluster_memory_writes_so_neither_is_lost()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var serializer = Serializer(harness);
        var key = RepoContextKeys.Memory("acme", "decisions", "shared");
        var accessor = RepoContextMemoryCodec.Accessor(MemoryTree(harness), key);

        // Cluster A wrote a title and a tag; cluster B concurrently added a different
        // tag, neither observing the other. Each write lands as its own dot.
        var a = new MemoryRecord
        {
            RepoId = "acme",
            Topic = "decisions",
            Id = "shared",
            Title = RepoContextValues.Lww("adopt hub-and-spoke", HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        };
        a.Tags.Add(Encoding.UTF8.GetBytes("from-a"), "clusterA", 0);
        var b = new MemoryRecord { RepoId = "acme", Topic = "decisions", Id = "shared" };
        b.Tags.Add(Encoding.UTF8.GetBytes("from-b"), "clusterB", 0);

        await accessor.MergeAsync(SingleWrite(serializer, "clusterA", a), Ct);
        await accessor.MergeAsync(SingleWrite(serializer, "clusterB", b), Ct);

        var view = await Store(harness).RecallAsync(key, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True);
            Assert.That(view.Fields["title"], Is.EqualTo("adopt hub-and-spoke"), "Cluster A's write survives.");
            Assert.That(view.Tags, Is.EquivalentTo(new[] { "from-a", "from-b" }),
                "Both clusters' concurrent writes fold together - no lost write.");
        });
    }

    [Test]
    public async Task Remember_with_a_ttl_preserves_the_expiry_across_the_crdt_write_path()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "todo", id: null, MemoryKind.Note, title: "time-boxed", body: null,
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: 600, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Expires, Is.True, "The TTL survives the MvRegister write path.");
            Assert.That(result.ExpiresAtUtc, Is.Not.Null);
        });

        var view = await store.RecallAsync(result.Key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True);
            Assert.That(view.Expires, Is.True);
            Assert.That(view.RemainingSeconds, Is.Not.Null.And.GreaterThan(0));
        });
    }

    [Test]
    public async Task Remember_without_a_ttl_stays_durable_across_the_crdt_write_path()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "decisions", id: null, MemoryKind.Decision, title: "durable", body: null,
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);

        Assert.That(result.Expires, Is.False, "A durable (no-TTL) memory write stays durable.");

        var view = await store.RecallAsync(result.Key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True);
            Assert.That(view.Expires, Is.False);
        });
    }

    [Test]
    public async Task Forget_hard_deletes_a_memory_entry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "notes", id: null, MemoryKind.Note, title: "ephemeral", body: null,
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        await store.ForgetAsync(result.Key, lapse: false, lapseSeconds: null, Ct);

        var view = await store.RecallAsync(result.Key, Ct);
        Assert.That(view.Exists, Is.False, "A hard forget removes the entry immediately.");
    }

    [Test]
    public async Task Forget_with_lapse_keeps_the_entry_readable_with_a_finite_life()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);

        var result = await store.RememberAsync(
            "acme", "notes", id: null, MemoryKind.Note, title: "draining", body: null,
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        // A soft-delete rides on CRDT-TTL: the entry stays live so concurrent readers
        // drain gracefully, but now carries a finite remaining life.
        await store.ForgetAsync(result.Key, lapse: true, lapseSeconds: 600, Ct);

        var view = await store.RecallAsync(result.Key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True, "A lapse keeps the entry readable while it drains.");
            Assert.That(view.Expires, Is.True);
            Assert.That(view.RemainingSeconds, Is.Not.Null.And.GreaterThan(0));
        });
    }
}
