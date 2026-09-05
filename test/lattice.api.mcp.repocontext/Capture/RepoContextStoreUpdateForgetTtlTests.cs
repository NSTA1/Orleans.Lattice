using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for the time-to-live-preserving branches of
/// <see cref="RepoContextStore.UpdateAsync"/> and
/// <see cref="RepoContextStore.ForgetAsync"/>, and the per-repository default TTL
/// path of <see cref="RepoContextStore.RememberAsync"/>. They pin that a patch of a
/// record carrying a remaining TTL (memory entry or structural node) rewrites it
/// with that remaining life rather than dropping the expiry, that a patch of a
/// durable node leaves it durable, that a lapse of a non-memory record sets a short
/// expiry, that a non-positive lapse window is rejected, that a lapse of a missing
/// key reports it did not exist, and that a remember with no explicit TTL inherits
/// the configured default.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreUpdateForgetTtlTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Structural(RepoContextMcpHarness harness)
        => harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);

    private static byte[] FileBytes(RepoContextMcpHarness harness, string path)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<FileNode>>();
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        return serializer.SerializeToArray(
            new FileNode { RepoId = RepoId, Path = path, Digest = RepoContextValues.Lww("d-" + path, clock) });
    }

    private static async Task<long> ExpiresAtTicksAsync(RepoContextMcpHarness harness, string key, CancellationToken ct)
    {
        var versioned = await Structural(harness).GetWithVersionAsync(key, ct);
        return versioned.ExpiresAtTicks;
    }

    [Test]
    public async Task Update_a_memory_entry_with_a_remaining_ttl_preserves_the_expiry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var remembered = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: 3600, Ct);

        var updated = await store.UpdateAsync(
            remembered.Key, fields: null, addTags: new[] { "seen" }, removeTags: null,
            addLinks: null, removeLinks: null, Ct);

        var memory = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);
        var versioned = await memory.GetWithVersionAsync(remembered.Key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(updated.Kind, Is.EqualTo("Memory"));
            Assert.That(updated.TagsAdded, Is.EqualTo(1), "The tag patch is applied.");
            Assert.That(versioned.ExpiresAtTicks, Is.Not.EqualTo(0L),
                "The patch rewrites the entry with its remaining time-to-live instead of dropping it.");
        });
    }

    [Test]
    public async Task Update_a_file_node_with_a_remaining_ttl_preserves_the_expiry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        await Structural(harness).SetAsync(key, FileBytes(harness, "src/A.cs"), TimeSpan.FromHours(1), Ct);

        var updated = await Store(harness).UpdateAsync(
            key, fields: null, addTags: new[] { "seen" }, removeTags: null, addLinks: null, removeLinks: null, Ct);

        var ticks = await ExpiresAtTicksAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(updated.Kind, Is.EqualTo("File"));
            Assert.That(updated.TagsAdded, Is.EqualTo(1));
            Assert.That(ticks, Is.Not.EqualTo(0L),
                "A patch of a TTL-bearing structural node preserves its expiry.");
        });
    }

    [Test]
    public async Task Update_a_file_node_without_a_ttl_stays_durable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = RepoContextKeys.File(RepoId, "src/B.cs");
        await Structural(harness).SetAsync(key, FileBytes(harness, "src/B.cs"), Ct);

        var updated = await Store(harness).UpdateAsync(
            key, fields: null, addTags: new[] { "seen" }, removeTags: null, addLinks: null, removeLinks: null, Ct);

        var ticks = await ExpiresAtTicksAsync(harness, key, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(updated.Kind, Is.EqualTo("File"));
            Assert.That(ticks, Is.EqualTo(0L),
                "A patch of a durable structural node leaves it durable.");
        });
    }

    [Test]
    public async Task Remember_without_an_explicit_ttl_applies_the_configured_default()
    {
        const string ttlRepo = "ttlrepo";
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureServices = services => services.Configure<RepoContextTtlOptions>(
                    ttlRepo, o => o.DefaultMemoryTtl = TimeSpan.FromHours(1)),
            }, Ct);

        var remembered = await Store(harness).RememberAsync(
            ttlRepo, "notes", id: "m1", MemoryKind.Note, title: "t", body: null,
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(remembered.Expires, Is.True,
                "A newly created memory entry inherits the per-repository default TTL when none is supplied.");
            Assert.That(remembered.ExpiresAtUtc, Is.Not.Null);
        });
    }

    [Test]
    public async Task Forget_with_a_non_positive_lapse_window_throws()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        Assert.That(
            async () => await Store(harness).ForgetAsync(
                RepoContextKeys.Memory(RepoId, "notes", "x"), lapse: true, lapseSeconds: 0, Ct),
            Throws.InstanceOf<McpException>(),
            "A lapse window of zero seconds is rejected.");
    }

    [Test]
    public async Task Forget_a_missing_key_with_lapse_reports_it_did_not_exist()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var result = await Store(harness).ForgetAsync(
            RepoContextKeys.Memory(RepoId, "notes", "ghost"), lapse: true, lapseSeconds: 60, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Existed, Is.False, "A lapse of a key with no live value reports it did not exist.");
            Assert.That(result.Mode, Is.EqualTo("lapse"));
            Assert.That(result.ExpiresAtUtc, Is.Null);
        });
    }

    [Test]
    public async Task Forget_a_file_node_with_lapse_sets_a_short_expiry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = RepoContextKeys.File(RepoId, "src/C.cs");
        await Structural(harness).SetAsync(key, FileBytes(harness, "src/C.cs"), Ct);

        var result = await Store(harness).ForgetAsync(key, lapse: true, lapseSeconds: 60, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Existed, Is.True, "A lapse of a live structural record reports it existed.");
            Assert.That(result.Mode, Is.EqualTo("lapse"));
            Assert.That(result.ExpiresAtUtc, Is.Not.Null, "A lapse rewrites the record with a short expiry.");
        });
    }
}
