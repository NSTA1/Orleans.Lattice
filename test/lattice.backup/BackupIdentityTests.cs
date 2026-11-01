namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupIdentity"/>: a backup id is a content address,
/// derived exactly as 10.2.0 derived it, so an identical capture re-derives the
/// same id (issue #5418); a full backup's scope-qualified id keeps identical
/// content in different scopes apart (issue #5620).
/// </summary>
[TestFixture]
public sealed class BackupIdentityTests
{
    // SHA-256 of the empty payload and of "abc".
    private const string EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private const string AbcPayloadHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    [Test]
    public void A_full_backup_id_is_the_10_2_0_payload_hash()
    {
        RedProof.PerCaptureIds = true; // RED-PROOF
        // The golden vector pins the 10.2.0 derivation: the id is the lowercase-hex
        // SHA-256 of the captured payload, and nothing else.
        Assert.Multiple(() =>
        {
            Assert.That(BackupIdentity.Full(BackupContentHash.Compute(Array.Empty<byte>())), Is.EqualTo(EmptyPayloadHash));
            Assert.That(BackupIdentity.Full(BackupContentHash.Compute("abc"u8.ToArray())), Is.EqualTo(AbcPayloadHash));
        });
    }

    [Test]
    public void An_increment_id_is_the_10_2_0_hash_of_its_base_and_payload_hash()
    {
        RedProof.PerCaptureIds = true; // RED-PROOF
        // SHA-256 of "<base id>\u001f<payload hash>", as 10.2.0's incremental
        // collector derived it.
        Assert.That(
            BackupIdentity.Increment(EmptyPayloadHash, AbcPayloadHash),
            Is.EqualTo("845cc52bd026514eb631f21b4306041d1f3ef74c31648e6a14441591c9cbd4b5"));
    }

    [Test]
    public void Identical_content_re_derives_the_same_id()
    {
        RedProof.PerCaptureIds = true; // RED-PROOF
        Assert.Multiple(() =>
        {
            Assert.That(BackupIdentity.Full(AbcPayloadHash), Is.EqualTo(BackupIdentity.Full(AbcPayloadHash)));
            Assert.That(
                BackupIdentity.Increment("base", AbcPayloadHash),
                Is.EqualTo(BackupIdentity.Increment("base", AbcPayloadHash)));
        });
    }

    [Test]
    public void An_increment_id_folds_in_its_base()
    {
        Assert.That(
            BackupIdentity.Increment("base-a", EmptyPayloadHash),
            Is.Not.EqualTo(BackupIdentity.Increment("base-b", EmptyPayloadHash)));
    }

    [Test]
    public void A_scope_qualified_id_is_deterministic_per_scope_and_distinct_from_the_bare_id()
    {
        var a = BackupScopeKey.For(BackupScopeSelector.WholeTree("tree-a"));
        var b = BackupScopeKey.For(BackupScopeSelector.WholeTree("tree-b"));

        Assert.Multiple(() =>
        {
            Assert.That(BackupIdentity.ScopeQualifiedFull(a, EmptyPayloadHash), Does.Match("^[0-9a-f]{64}$"));
            Assert.That(BackupIdentity.ScopeQualifiedFull(a, EmptyPayloadHash), Is.EqualTo(BackupIdentity.ScopeQualifiedFull(a, EmptyPayloadHash)));
            Assert.That(BackupIdentity.ScopeQualifiedFull(a, EmptyPayloadHash), Is.Not.EqualTo(BackupIdentity.ScopeQualifiedFull(b, EmptyPayloadHash)));
            Assert.That(BackupIdentity.ScopeQualifiedFull(a, EmptyPayloadHash), Is.Not.EqualTo(EmptyPayloadHash));
        });
    }

    [Test]
    public void An_empty_input_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupIdentity.Full(""), Throws.ArgumentException);
            Assert.That(() => BackupIdentity.Increment("", AbcPayloadHash), Throws.ArgumentException);
            Assert.That(() => BackupIdentity.Increment("base", ""), Throws.ArgumentException);
            Assert.That(() => BackupIdentity.ScopeQualifiedFull("", AbcPayloadHash), Throws.ArgumentException);
            Assert.That(() => BackupIdentity.ScopeQualifiedFull("scope", ""), Throws.ArgumentException);
        });
    }
}
