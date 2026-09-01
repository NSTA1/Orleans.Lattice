namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the source-preparation control artefact: the three outcomes a strategy
/// can report, and the invariants that keep a failed preparation from being mistaken
/// for a runnable one.
/// </summary>
[TestFixture]
public sealed class RepoContextSourcePreparationTests
{
    private static RepoIndexJobRequest Request() => new()
    {
        RepoId = "acme",
        RepoRoot = Path.GetTempPath(),
    };

    [Test]
    public void Proceed_carries_the_request_and_the_anchor()
    {
        var request = Request();

        var preparation = RepoContextSourcePreparation.Proceed(
            RepoContextSourceKind.GitRemote, request, "abc123");

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Kind, Is.EqualTo(RepoContextSourceKind.GitRemote));
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Proceed));
            Assert.That(preparation.Request, Is.SameAs(request));
            Assert.That(preparation.CommitSha, Is.EqualTo("abc123"));
            Assert.That(preparation.FailureReason, Is.Null);
        });
    }

    [Test]
    public void Proceed_rejects_a_null_request()
    {
        Assert.That(
            () => RepoContextSourcePreparation.Proceed(RepoContextSourceKind.GitRemote, null!, "abc"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UpToDate_starts_no_run()
    {
        var preparation = RepoContextSourcePreparation.UpToDate(RepoContextSourceKind.GitRemote, "abc123");

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.UpToDate));
            Assert.That(preparation.Request, Is.Null);
            Assert.That(preparation.CommitSha, Is.EqualTo("abc123"));
        });
    }

    [Test]
    public void Failed_stamps_no_anchor_and_starts_no_run()
    {
        var preparation = RepoContextSourcePreparation.Failed(RepoContextSourceKind.GitRemote, "no credential");

        Assert.Multiple(() =>
        {
            Assert.That(preparation.Outcome, Is.EqualTo(RepoContextSourceOutcome.Failed));
            Assert.That(preparation.Request, Is.Null);
            Assert.That(preparation.CommitSha, Is.Null,
                "A failed generation must never leave an anchor claiming a revision it did not serve.");
            Assert.That(preparation.FailureReason, Is.EqualTo("no credential"));
        });
    }

    [Test]
    public void Failed_rejects_a_null_reason()
    {
        Assert.That(
            () => RepoContextSourcePreparation.Failed(RepoContextSourceKind.GitRemote, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Source_kinds_and_outcomes_have_stable_default_members()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)RepoContextSourceKind.MountedWorkspace, Is.Zero,
                "The mounted workspace is the default strategy.");
            Assert.That((int)RepoContextSourceOutcome.Proceed, Is.Zero);
            Assert.That((int)RepoContextGitAuthMode.Token, Is.Zero,
                "Token auth is the default; anonymous is an explicit opt-in.");
        });
    }
}
