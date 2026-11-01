using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A full backup's id is the content address of its payload, which names no tree
/// or scope, so identical content captured in two scopes would share one id, and
/// the second registration would take over the first's catalog row (issue #5620).
/// The second scope takes a scope-qualified id instead, decided under a per-id
/// claim, and keeps re-deriving it.
/// </summary>
[Category("Integration")]
public sealed class BackupIdCollisionTests
{
    private const string TreeA = "tree-a";

    private const string TreeB = "tree-b";

    // SHA-256 of the empty payload: the 10.2.0 id of every empty full backup.
    private const string EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new CaptureClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Identical_content_in_two_scopes_lists_one_backup_in_each_scope()
    {
        // Two empty scopes - a prefix nothing is written under, on each of two
        // trees - capture the same, empty, payload.
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeA).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await _fixture.GrainFactory.GetGrain<ILattice>(TreeB).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var scopeA = BackupScopeSelector.Prefix(TreeA, "empty/");
        var scopeB = BackupScopeSelector.Prefix(TreeB, "empty/");

        var a = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest("a", scopeA));
        var b = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest("b", scopeB));
        var aAgain = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest("a-again", scopeA));
        var bAgain = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest("b-again", scopeB));

        var rowA = await _fixture.Catalog.GetAsync(a.BackupId);
        var rowB = await _fixture.Catalog.GetAsync(b.BackupId);
        Assert.Multiple(async () =>
        {
            Assert.That(a.BackupId, Is.EqualTo(EmptyPayloadHash), "the first scope keeps the 10.2.0 content address");
            Assert.That(
                b.BackupId,
                Is.EqualTo(BackupIdentity.ScopeQualifiedFull(BackupScopeKey.For(scopeB), EmptyPayloadHash)),
                "the second scope takes its scope-qualified id");
            Assert.That(aAgain.BackupId, Is.EqualTo(a.BackupId), "a same-scope retry re-derives the bare id");
            Assert.That(bAgain.BackupId, Is.EqualTo(b.BackupId), "a same-scope retry re-derives the scope-qualified id");
            Assert.That(rowA?.Scope, Is.EqualTo(scopeA), "the first scope's row keeps its scope");
            Assert.That(rowB?.Scope, Is.EqualTo(scopeB), "the second scope's row is its own");
            Assert.That((await _fixture.Sink.ProbeAsync(a.BackupId)).IsResolvable, Is.True);
            Assert.That((await _fixture.Sink.ProbeAsync(b.BackupId)).IsResolvable, Is.True);
        });
    }

    [Test]
    public async Task A_bare_id_claim_admits_one_scope_until_it_is_released()
    {
        // Nothing durable holds this id, so only the pending claim decides.
        var claim = _fixture.GrainFactory.GetGrain<IBackupIdClaimGrain>("unclaimed-id");

        Assert.Multiple(async () =>
        {
            Assert.That(await claim.TryClaimAsync("scope-a"), Is.True, "the first scope claims the id");
            Assert.That(await claim.TryClaimAsync("scope-a"), Is.True, "the claiming scope keeps it");
            Assert.That(await claim.TryClaimAsync("scope-b"), Is.False, "another scope is refused while the claim is pending");
        });

        await claim.ReleaseAsync("scope-b");
        Assert.That(await claim.TryClaimAsync("scope-b"), Is.False, "only the claiming scope can release its claim");

        await claim.ReleaseAsync("scope-a");
        Assert.That(await claim.TryClaimAsync("scope-b"), Is.True, "a released claim admits the next scope");
    }

    [Test]
    public async Task A_scope_keeps_the_bare_id_until_its_last_claim_is_released()
    {
        RedProof.ReleaseClearsAll = true; // RED-PROOF
        // Two publishes of one scope hold one claim each (issue #5627): the first
        // to finish must not release the other's.
        var claim = _fixture.GrainFactory.GetGrain<IBackupIdClaimGrain>("twice-claimed-id");
        Assert.That(await claim.TryClaimAsync("scope-a"), Is.True);
        Assert.That(await claim.TryClaimAsync("scope-a"), Is.True);

        await claim.ReleaseAsync("scope-a");
        Assert.That(await claim.TryClaimAsync("scope-b"), Is.False, "scope A still holds a claim");

        await claim.ReleaseAsync("scope-a");
        Assert.That(await claim.TryClaimAsync("scope-b"), Is.True, "every claim of scope A is released");
    }

    [Test]
    public async Task The_owning_scope_holds_a_claim_while_it_republishes_its_id()
    {
        RedProof.OwnerNoPending = true; // RED-PROOF
        // The owner's own claim is recorded (issue #5627): while its publish runs,
        // and even if the id is retired meanwhile, no other scope can take it.
        var captured = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("owned", BackupScopeSelector.Prefix(TreeA, "owned/")));
        var ownerScope = BackupScopeKey.For(captured.Manifest.Scope);
        var claim = _fixture.GrainFactory.GetGrain<IBackupIdClaimGrain>(captured.BackupId);

        Assert.That(await claim.TryClaimAsync(ownerScope), Is.True, "the owner claims its own id");
        await _fixture.Catalog.RemoveAsync(captured.BackupId);
        await _fixture.Sink.DeleteManifestAsync(captured.BackupId);

        Assert.That(await claim.TryClaimAsync("another-scope"), Is.False, "the owner's claim keeps the ownerless id");
        await claim.ReleaseAsync(ownerScope);
    }
}
