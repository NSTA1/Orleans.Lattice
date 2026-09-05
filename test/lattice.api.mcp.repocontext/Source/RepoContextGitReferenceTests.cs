namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the ref normalisation the transport depends on, and for the in-memory
/// control artefact a source strategy returns.
/// </summary>
[TestFixture]
public sealed class RepoContextGitReferenceTests
{
    [TestCase("main", "refs/heads/main")]
    [TestCase(" release/v1 ", "refs/heads/release/v1")]
    [TestCase("refs/heads/topic", "refs/heads/topic")]
    [TestCase("refs/tags/v1.0.0", "refs/tags/v1.0.0")]
    [TestCase("", "refs/heads/main")]
    public void Qualify_expands_a_bare_name_to_a_branch_ref(string reference, string expected)
    {
        Assert.That(RepoContextGitReference.Qualify(reference), Is.EqualTo(expected));
    }

    [Test]
    public void Qualify_rejects_a_null_reference()
    {
        Assert.That(() => RepoContextGitReference.Qualify(null!), Throws.ArgumentNullException);
    }

    [TestCase("main", "refs/remotes/origin/main")]
    [TestCase("refs/heads/topic", "refs/remotes/origin/topic")]
    [TestCase("refs/tags/v1.0.0", "refs/tags/v1.0.0")]
    public void LocalTrackingRef_mirrors_a_branch_under_the_remote_namespace(string reference, string expected)
    {
        Assert.That(RepoContextGitReference.LocalTrackingRef(reference), Is.EqualTo(expected));
    }

    [Test]
    public void RefSpec_is_forced_so_a_rewritten_branch_still_converges()
    {
        Assert.That(
            RepoContextGitReference.RefSpec("main"),
            Is.EqualTo("+refs/heads/main:refs/remotes/origin/main"));
    }

    [TestCase("refs/tags/v1", true)]
    [TestCase("main", false)]
    [TestCase("refs/heads/main", false)]
    public void IsTag_recognises_the_tag_namespace(string reference, bool expected)
    {
        Assert.That(RepoContextGitReference.IsTag(reference), Is.EqualTo(expected));
    }

    [Test]
    public void DefaultReference_is_the_main_branch()
    {
        Assert.That(RepoContextGitReference.DefaultReference, Is.EqualTo("refs/heads/main"));
    }
}
