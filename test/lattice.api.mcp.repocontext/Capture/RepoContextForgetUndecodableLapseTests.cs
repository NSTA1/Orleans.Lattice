using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for retiring a memory entry whose stored value cannot be
/// decoded.
/// <para>
/// A malformed stored value used to foreclose every remedy but destruction: a lapse
/// is itself a read-modify-write, so it failed on exactly the records that most
/// needed retiring, leaving a hard delete - which loses the entry rather than its
/// formatting - as the only way to clear one. These tests pin that a lapse now
/// succeeds over an undecodable value, that it reports having done so, and that the
/// tolerance stops at retirement: a mutation over the same record still fails loudly.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextForgetUndecodableLapseTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Memory(RepoContextMcpHarness harness)
        => harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);

    /// <summary>
    /// Remembers an entry, then overwrites its stored value with bytes that decode as
    /// nothing, reproducing the state a record reaches when an upstream rewrite
    /// mangles its stored representation.
    /// </summary>
    private static async Task<string> SeedUndecodableAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var remembered = await Store(harness).RememberAsync(
            RepoId, "gotchas", id: "corrupt-1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, ct);

        await Memory(harness).SetAsync(
            remembered.Key, new byte[] { 0xFE, 0x01, 0x00, 0x00, 0x00, 0x07 }, ct);

        return remembered.Key;
    }

    [Test]
    public async Task A_lapse_retires_an_undecodable_record_instead_of_failing()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedUndecodableAsync(harness, Ct);

        var forgotten = await Store(harness).ForgetAsync(
            key, lapse: true, lapseSeconds: 60, fencingToken: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(forgotten.Mode, Is.EqualTo("lapse"));
            Assert.That(forgotten.Existed, Is.True);
            Assert.That(forgotten.Undecodable, Is.True,
                "The tolerance is reported, so a store can never quietly shed records it could not read.");
            Assert.That(forgotten.ExpiresAtUtc, Is.Not.Null,
                "The entry really carries the short expiry: it was retired, not merely reported as retired.");
        });
    }

    [Test]
    public async Task A_lapse_of_a_healthy_record_does_not_report_it_as_undecodable()
    {
        // The control arm. Without it, an implementation that reported every lapse as
        // undecodable would pass the test above, and the flag would carry no signal.
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var remembered = await Store(harness).RememberAsync(
            RepoId, "gotchas", id: "healthy-1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        var forgotten = await Store(harness).ForgetAsync(
            remembered.Key, lapse: true, lapseSeconds: 60, fencingToken: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(forgotten.Undecodable, Is.False);
            Assert.That(forgotten.ExpiresAtUtc, Is.Not.Null);
        });
    }

    [Test]
    public async Task A_mutation_over_an_undecodable_record_still_fails_and_names_the_key()
    {
        // The tolerance is scoped to retirement. A read-modify-write that is not a
        // retirement must still fail loudly, or "this record could not be decoded"
        // would quietly become a normal outcome across the whole write surface.
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedUndecodableAsync(harness, Ct);

        var ex = Assert.ThrowsAsync<RepoContextRecordDecodeException>(
            async () => await Store(harness).UpdateAsync(
                key, fields: null, addTags: new[] { "seen" }, removeTags: null,
                addLinks: null, removeLinks: null, Ct));

        Assert.That(ex!.Key, Is.EqualTo(key),
            "The mutation fails, and unlike before it names the record that stopped it.");
    }

    [Test]
    public async Task A_hard_delete_of_an_undecodable_record_still_removes_it()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var key = await SeedUndecodableAsync(harness, Ct);

        var forgotten = await Store(harness).ForgetAsync(
            key, lapse: false, lapseSeconds: null, fencingToken: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(forgotten.Mode, Is.EqualTo("delete"));
            Assert.That(forgotten.Existed, Is.True);
            Assert.That(forgotten.Undecodable, Is.False,
                "A hard delete never decodes the value, so it makes no claim about whether it was readable.");
        });
    }

    [Test]
    public async Task A_claim_on_an_undecodable_record_still_fences_the_retirement()
    {
        // The fence over an undecodable record is resolved against the lock rather
        // than the record's unreadable stamp. Without this arm, the tolerance that
        // makes retirement possible would be indistinguishable from having removed
        // the fence on exactly the write that most needs one.
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var remembered = await store.RememberAsync(
            RepoId, "gotchas", id: "claimed-1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        var claim = await store.ClaimAsync(
            remembered.Key, owner: "worker-a", leaseSeconds: 120, maxWaitSeconds: null, Ct);
        Assert.That(claim.Granted, Is.True, "The claim is the precondition this arm tests against.");

        await Memory(harness).SetAsync(
            remembered.Key, new byte[] { 0xFE, 0x01, 0x00, 0x00, 0x00, 0x07 }, Ct);

        var refused = Assert.ThrowsAsync<RepoContextClaimConflictException>(
            async () => await store.ForgetAsync(
                remembered.Key, lapse: true, lapseSeconds: 60, fencingToken: null, Ct));

        Assert.That(refused!.CurrentFencingToken, Is.EqualTo(claim.FencingToken),
            "The refusal reports the lock's live token, which is the authority the record could no longer supply.");

        // The holder is still entitled, so the retirement it is entitled to must land.
        var forgotten = await store.ForgetAsync(
            remembered.Key, lapse: true, lapseSeconds: 60, fencingToken: claim.FencingToken, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(forgotten.Undecodable, Is.True);
            Assert.That(forgotten.ExpiresAtUtc, Is.Not.Null);
        });
    }
}
