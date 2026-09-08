namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="EmbeddingRepoContextVectorIngestor.GapSetDigest"/>, the
/// order-independent fingerprint of the unchanged files the embedding back-fill
/// selects as uncovered gaps on a pass. The digest is the discriminating instrument
/// the flat gap <i>count</i> in the pass log cannot supply: on the live deployment a
/// quiet pass re-embeds a steady set of unchanged files indefinitely, and only the
/// <i>identity</i> of that set across consecutive quiet passes tells a broken presence
/// check (the same files every pass, so a stable digest) from ongoing vector loss (a
/// rotating set, so a changing digest) apart. These tests pin the two properties that
/// make the digest trustworthy for that comparison: it depends on which files were
/// selected and not on the order they were walked, and it changes when the set
/// changes.
/// </summary>
/// <remarks>
/// Pure in-process function test: it constructs <see cref="RepoFileEntry"/> values
/// directly and calls the digest, standing up no silo and reading no file, so it
/// needs no slow category and runs in the fast dev loop.
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorGapDigestTests
{
    private const string RepoId = "acme";

    private static RepoFileEntry Entry(string relativePath)
        => new(relativePath, "digest-" + relativePath, relativePath.Length, "csharp");

    [Test]
    public void GapSetDigest_of_an_empty_set_is_zero()
    {
        Assert.That(
            EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, Array.Empty<RepoFileEntry>()),
            Is.EqualTo(0UL));
    }

    [Test]
    public void GapSetDigest_is_order_independent()
    {
        var forward = new[] { Entry("src/a.cs"), Entry("src/b.cs"), Entry("src/c.cs") };
        var shuffled = new[] { Entry("src/c.cs"), Entry("src/a.cs"), Entry("src/b.cs") };

        // The set-digest must not depend on the walk order, or two quiet passes that
        // re-select the same files in a different order would look like a rotating set
        // and falsely point at ongoing vector loss.
        Assert.That(
            EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, shuffled),
            Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, forward)));
    }

    [Test]
    public void GapSetDigest_is_stable_for_the_same_set_rebuilt_independently()
    {
        var passOne = new[] { Entry("a.cs"), Entry("dir/b.cs"), Entry("dir/nested/c.cs") };
        var passTwo = new[] { Entry("a.cs"), Entry("dir/b.cs"), Entry("dir/nested/c.cs") };

        // This is the "same files re-embedded every quiet pass" case that indicates a
        // broken presence check: independently built lists of the same paths hash equal.
        Assert.That(
            EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, passTwo),
            Is.EqualTo(EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, passOne)));
    }

    [Test]
    public void GapSetDigest_changes_when_a_single_file_rotates_out()
    {
        var passOne = new[] { Entry("src/a.cs"), Entry("src/b.cs"), Entry("src/c.cs") };
        var rotated = new[] { Entry("src/a.cs"), Entry("src/b.cs"), Entry("src/d.cs") };

        // This is the "rotating set" case that indicates ongoing vector loss: swapping
        // one file for another must move the digest, or the instrument could not tell
        // hypothesis (b) from hypothesis (a).
        Assert.That(
            EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, rotated),
            Is.Not.EqualTo(EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, passOne)));
    }

    [Test]
    public void GapSetDigest_is_repository_scoped()
    {
        var files = new[] { Entry("src/a.cs"), Entry("src/b.cs") };

        // The digest is taken over the same repo-scoped source ids the coverage check
        // uses, so the identical relative paths under two repositories are distinct
        // sets and must not collide.
        Assert.That(
            EmbeddingRepoContextVectorIngestor.GapSetDigest("other", files),
            Is.Not.EqualTo(EmbeddingRepoContextVectorIngestor.GapSetDigest(RepoId, files)));
    }
}
