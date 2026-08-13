using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the membership and retirement surface of
/// <see cref="RepoContextVectorWriter"/> against a live in-memory Lattice cluster:
/// <see cref="RepoContextVectorWriter.AddMembersAsync"/> is a batched, idempotent
/// read-modify-write; <see cref="RepoContextVectorWriter.LoadEmbeddedMembersAsync"/>
/// returns the add-wins presence set; and
/// <see cref="RepoContextVectorWriter.RetireAsync"/> deletes a source's metadata
/// presence keys and observed-removes it from the set so a deleted file drops its
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

    private static byte[] SourceIdBytes(string sourceKey)
        => Encoding.UTF8.GetBytes(VectorCodec.SourceId(sourceKey));

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
            Assert.That(members.Contains(SourceIdBytes(keyA)), Is.True, "A's source id is a live member.");
            Assert.That(members.Contains(SourceIdBytes(keyB)), Is.True, "B's source id is a live member.");
            Assert.That(members.Contains(SourceIdBytes(RepoContextKeys.File(RepoId, "src/C.cs"))), Is.False,
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

        Assert.That(members.Elements(), Is.Empty, "No membership record yet means an empty presence set, not an error.");
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
            Assert.That(members.Contains(SourceIdBytes(key)), Is.True);
            Assert.That(members.Elements().Count(), Is.EqualTo(1),
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
        Assert.That(members.Elements(), Is.Empty, "An empty batch writes no membership record.");
    }

    [Test]
    public async Task StoreAsync_persists_a_vector_without_recording_membership()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, grains) = Resolve(harness);

        var key = RepoContextKeys.File(RepoId, "src/A.cs");
        var vectorId = await writer.StoreAsync(RepoId, key, Space, new float[] { 1f, 0f, 0f, 0f }, Ct);

        var metadata = grains.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(metadata.ExistsAsync(RepoContextKeys.Vector(RepoId, vectorId), Ct).Result, Is.True,
                "The metadata presence key lands on the store.");
            Assert.That(members.Contains(SourceIdBytes(key)), Is.False,
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
        var vectorId = await writer.StoreAsync(RepoId, key, Space, new float[] { 1f, 0f, 0f, 0f }, Ct);
        await writer.AddMembersAsync(RepoId, new[] { key }, Ct);

        await writer.RetireAsync(RepoId, key, Ct);

        var metadata = grains.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(metadata.ExistsAsync(RepoContextKeys.Vector(RepoId, vectorId), Ct).Result, Is.False,
                "Retire deletes the source's metadata presence key.");
            Assert.That(members.Contains(SourceIdBytes(key)), Is.False,
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
        Assert.That(members.Elements(), Is.Empty);
    }

    [Test]
    public async Task RetireAsync_leaves_other_sources_membership_intact()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (writer, _) = Resolve(harness);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.StoreAsync(RepoId, keyA, Space, new float[] { 1f, 0f, 0f, 0f }, Ct);
        await writer.StoreAsync(RepoId, keyB, Space, new float[] { 0f, 1f, 0f, 0f }, Ct);
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);

        await writer.RetireAsync(RepoId, keyA, Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(members.Contains(SourceIdBytes(keyA)), Is.False, "The retired source is gone.");
            Assert.That(members.Contains(SourceIdBytes(keyB)), Is.True, "The untouched source stays a live member.");
        });
    }
}
