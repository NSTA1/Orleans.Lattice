using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the membership and retirement surface of
/// <see cref="RepoContextVectorWriter"/> against a live in-memory Lattice cluster:
/// <see cref="RepoContextVectorWriter.AddMembersAsync"/> enables one add-wins
/// presence flag per source; <see cref="RepoContextVectorWriter.LoadEmbeddedMembersAsync"/>
/// returns the set of source ids whose flag is enabled; and
/// <see cref="RepoContextVectorWriter.RetireAsync"/> deletes a source's metadata
/// presence keys and disables its membership flag so a deleted file drops its
/// vector and the tally stays honest. These are the invariants the embedding
/// back-fill's gap detection and the retire-on-delete path rely on.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved vector trees) via <see cref="RepoContextMcpHarness"/>,
/// so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorWriterMembershipTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpace Space = new("fake-embed-v1", 4, normalized: true);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static string SourceId(string sourceKey)
        => VectorCodec.SourceId(sourceKey);

    // The deterministic vector id the writer forms for a source's first (unit 0)
    // passage, so a test can address the stored metadata presence key now that
    // StoreAsync writes per-unit keys and no longer returns the id.
    private static string ExpectedVectorId(string sourceKey, ReadOnlyMemory<float> vector)
    {
        var contentAddress = VectorCodec.ContentAddress(VectorCodec.Encode(vector));
        return RepoContextVectorWriter.FormatVectorId(VectorCodec.SourceId(sourceKey), 0, contentAddress);
    }

    private static (RepoContextVectorWriter Writer, IGrainFactory Grains) Resolve(RepoContextMcpHarness harness)
        => (harness.Services.GetRequiredService<RepoContextVectorWriter>(), harness.GrainFactory);

    [Test]
    public async Task AddMembersAsync_records_presence_that_LoadEmbeddedMembersAsync_reports()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(members.Contains(SourceId(keyA)), Is.True, "A's source id is a live member.");
            Assert.That(members.Contains(SourceId(keyB)), Is.True, "B's source id is a live member.");
            Assert.That(members.Contains(SourceId(RepoContextKeys.File(RepoId, "src/C.cs"))), Is.False,
                "A source never added is not a member.");
        });
    }

    [Test]
    public async Task LoadEmbeddedMembersAsync_returns_an_empty_set_for_a_repo_that_embedded_nothing()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var members = await writer.LoadEmbeddedMembersAsync("never-embedded", Ct);

        Assert.That(members, Is.Empty, "No membership record yet means an empty presence set, not an error.");
    }

    [Test]
    public async Task AddMembersAsync_is_idempotent_for_an_already_present_source()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(members.Contains(SourceId(key)), Is.True);
            Assert.That(members.Count, Is.EqualTo(1),
                "Re-adding the same source is a no-op: the set has exactly one member, not a duplicate.");
        });
    }

    [Test]
    public async Task AddMembersAsync_ignores_an_empty_batch()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        await writer.AddMembersAsync(RepoId, Array.Empty<string>(), Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.That(members, Is.Empty, "An empty batch writes no membership record.");
    }

    [Test]
    public async Task StoreAsync_persists_a_vector_without_recording_membership()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, grains) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        var vector = new float[] { 1f, 0f, 0f, 0f };
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[] { vector }, Ct);
        var vectorId = ExpectedVectorId(key, vector);

        var metadata = grains.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(metadata.ExistsAsync(RepoContextKeys.Vector(RepoId, vectorId), Ct).Result, Is.True,
                "The metadata presence key lands on the store.");
            Assert.That(members.Contains(SourceId(key)), Is.False,
                "Membership is the caller's per-batch responsibility (AddMembersAsync), not folded by StoreAsync.");
        });
    }

    [Test]
    public async Task RetireAsync_deletes_the_vector_and_removes_the_member()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, grains) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        var vector = new float[] { 1f, 0f, 0f, 0f };
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[] { vector }, Ct);
        var vectorId = ExpectedVectorId(key, vector);
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);

        await writer.RetireAsync(RepoId, key, Ct);

        var metadata = grains.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(metadata.ExistsAsync(RepoContextKeys.Vector(RepoId, vectorId), Ct).Result, Is.False,
                "Retire deletes the source's metadata presence key.");
            Assert.That(members.Contains(SourceId(key)), Is.False,
                "Retire observed-removes the source from the membership set so the tally stays honest.");
        });
    }

    [Test]
    public async Task RetireAsync_is_a_no_op_for_a_source_that_was_never_embedded()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        // Retiring a source with no live vector must not throw and must leave the
        // (empty) membership set unchanged.
        await writer.RetireAsync(RepoId, RepoContextKeys.File(RepoId, "src/ghost.cs"), Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.That(members, Is.Empty);
    }

    [Test]
    public async Task RetireAsync_leaves_other_sources_membership_intact()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.StoreAsync(RepoId, keyA, Space, new ReadOnlyMemory<float>[] { new float[] { 1f, 0f, 0f, 0f } }, Ct);
        await writer.StoreAsync(RepoId, keyB, Space, new ReadOnlyMemory<float>[] { new float[] { 0f, 1f, 0f, 0f } }, Ct);
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);

        await writer.RetireAsync(RepoId, keyA, Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(members.Contains(SourceId(keyA)), Is.False, "The retired source is gone.");
            Assert.That(members.Contains(SourceId(keyB)), Is.True, "The untouched source stays a live member.");
        });
    }

    [Test]
    public async Task AddMembersAsync_after_retire_makes_the_source_live_again()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);
        await writer.RetireAsync(RepoId, key, Ct);
        // Re-adding after a retire must re-enable the flag: the enable authors a
        // fresh dot that outlives the disable's tombstones, so an add-wins flag
        // becomes live again rather than staying disabled.
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(members.Contains(SourceId(key)), Is.True,
                "A source re-added after retirement is a live member again (enable-wins over the prior disable).");
            Assert.That(members.Count, Is.EqualTo(1), "There is exactly one live member, not a duplicate.");
        });
    }

    [Test]
    public async Task StoreAsync_persists_one_presence_key_per_passage()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        var vectors = new ReadOnlyMemory<float>[]
        {
            new float[] { 1f, 0f, 0f, 0f },
            new float[] { 0f, 1f, 0f, 0f },
            new float[] { 0f, 0f, 1f, 0f },
        };
        await writer.StoreAsync(RepoId, key, Space, vectors, Ct);

        var live = await LiveVectorKeysAsync(harness, key);
        Assert.That(live, Has.Count.EqualTo(3),
            "A three-passage source stores three live metadata presence keys, one per unit.");
    }

    [Test]
    public async Task StoreAsync_retires_units_the_source_no_longer_has_when_it_shrinks()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[]
        {
            new float[] { 1f, 0f, 0f, 0f },
            new float[] { 0f, 1f, 0f, 0f },
            new float[] { 0f, 0f, 1f, 0f },
        }, Ct);

        // Re-embed with fewer passages: the source lost content, so the trailing
        // units must be retired and only the current set left live.
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[]
        {
            new float[] { 1f, 0f, 0f, 0f },
        }, Ct);

        var live = await LiveVectorKeysAsync(harness, key);
        Assert.That(live, Has.Count.EqualTo(1),
            "Shrinking the passage set retires the units the source no longer has.");
    }

    [Test]
    public async Task StoreAsync_replaces_a_units_key_when_its_content_changes()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        var first = new float[] { 1f, 0f, 0f, 0f };
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[] { first }, Ct);
        var firstId = ExpectedVectorId(key, first);

        var second = new float[] { 0f, 1f, 0f, 0f };
        await writer.StoreAsync(RepoId, key, Space, new ReadOnlyMemory<float>[] { second }, Ct);
        var secondId = ExpectedVectorId(key, second);

        var live = await LiveVectorKeysAsync(harness, key);
        Assert.Multiple(() =>
        {
            Assert.That(live, Has.Count.EqualTo(1), "A content-changed unit leaves exactly one live key.");
            Assert.That(live, Does.Contain(RepoContextKeys.Vector(RepoId, secondId)),
                "The live key addresses the new content.");
            Assert.That(live, Does.Not.Contain(RepoContextKeys.Vector(RepoId, firstId)),
                "The superseded content address is retired.");
        });
    }

    [Test]
    public void FormatVectorId_zero_pads_the_unit_and_groups_by_source_prefix()
    {
        var id = RepoContextVectorWriter.FormatVectorId("0123456789abcdef", 7, "deadbeef");

        Assert.That(id, Is.EqualTo("0123456789abcdef.0007.deadbeef"),
            "The unit ordinal is fixed-width so lexical order matches numeric order under the shared source prefix.");
    }

    private static async Task<List<string>> LiveVectorKeysAsync(RepoContextMcpHarness harness, string sourceKey)
    {
        var sourceId = VectorCodec.SourceId(sourceKey);
        var prefix = $"{RepoContextKeys.VectorsPrefix(RepoId)}{sourceId}.";
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var keys = new List<string>();
        string? token = null;
        do
        {
            var page = await RepoContextPortability.EnumerateAsync(
                tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, CancellationToken.None);
            keys.AddRange(page.Records.Select(r => r.Key));
            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        return keys;
    }
}
