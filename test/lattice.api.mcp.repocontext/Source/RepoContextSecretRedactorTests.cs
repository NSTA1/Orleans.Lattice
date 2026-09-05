namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the secret redactor every message leaving the git source passes
/// through. A transport exception routinely quotes the remote URL it was handed, so
/// both an embedded userinfo component and the resolved credential's secret must be
/// scrubbed before the text reaches a log or a failure reason.
/// </summary>
[TestFixture]
public sealed class RepoContextSecretRedactorTests
{
    private const string SchemeSeparator = "://";

    private static string Url(string userinfo, string host) =>
        "https" + SchemeSeparator + (userinfo.Length == 0 ? string.Empty : userinfo + "@") + host;

    [Test]
    public void Redact_removes_the_credential_secret()
    {
        var credential = RepoContextGitCredential.Token("ghp-not-a-real-token");

        var scrubbed = RepoContextSecretRedactor.Redact(
            "authentication rejected for ghp-not-a-real-token", credential);

        Assert.Multiple(() =>
        {
            Assert.That(scrubbed, Does.Not.Contain("ghp-not-a-real-token"));
            Assert.That(scrubbed, Does.Contain(RepoContextSecretRedactor.Placeholder));
        });
    }

    [Test]
    public void Redact_strips_userinfo_from_a_url()
    {
        var scrubbed = RepoContextSecretRedactor.Redact(
            "failed to fetch " + Url("user:hunter2", "git.example.invalid/acme.git"), credential: null);

        Assert.Multiple(() =>
        {
            Assert.That(scrubbed, Does.Not.Contain("hunter2"));
            Assert.That(scrubbed, Does.Contain("git.example.invalid/acme.git"));
            Assert.That(scrubbed, Does.Contain(RepoContextSecretRedactor.Placeholder));
        });
    }

    [Test]
    public void Redact_returns_empty_for_blank_input()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextSecretRedactor.Redact(null, credential: null), Is.Empty);
            Assert.That(RepoContextSecretRedactor.Redact("   ", credential: null), Is.Empty);
        });
    }

    [Test]
    public void Redact_leaves_an_anonymous_credential_alone()
    {
        var scrubbed = RepoContextSecretRedactor.Redact("plain message", RepoContextGitCredential.Anonymous);

        Assert.That(scrubbed, Is.EqualTo("plain message"));
    }

    [Test]
    public void RedactUrls_rejects_null_text()
    {
        Assert.That(() => RepoContextSecretRedactor.RedactUrls(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void RedactUrls_leaves_text_without_a_scheme_untouched()
    {
        Assert.That(RepoContextSecretRedactor.RedactUrls("no url here"), Is.EqualTo("no url here"));
    }

    [Test]
    public void RedactUrls_leaves_a_url_without_userinfo_untouched()
    {
        var url = Url(string.Empty, "git.example.invalid/acme.git");

        Assert.That(RepoContextSecretRedactor.RedactUrls(url), Is.EqualTo(url));
    }

    [Test]
    public void RedactUrls_scrubs_every_url_in_the_text()
    {
        var text = Url("a:1", "one.invalid/x") + " then " + Url("b:2", "two.invalid/y");

        var scrubbed = RepoContextSecretRedactor.RedactUrls(text);

        Assert.Multiple(() =>
        {
            Assert.That(scrubbed, Does.Not.Contain("a:1"));
            Assert.That(scrubbed, Does.Not.Contain("b:2"));
            Assert.That(scrubbed, Does.Contain("one.invalid/x"));
            Assert.That(scrubbed, Does.Contain("two.invalid/y"));
        });
    }

    [Test]
    public void RedactUrls_preserves_trailing_text_after_the_authority()
    {
        var text = Url("u:p", "host.invalid") + "/path, and more prose";

        Assert.That(RepoContextSecretRedactor.RedactUrls(text), Does.EndWith("/path, and more prose"));
    }

    [TestCase("user:p,ss", "p,ss", TestName = "RedactUrls_redacts_userinfo_containing_a_comma")]
    [TestCase("user:p;ss", "p;ss", TestName = "RedactUrls_redacts_userinfo_containing_a_semicolon")]
    [TestCase("user:p(ss", "p(ss", TestName = "RedactUrls_redacts_userinfo_containing_an_open_paren")]
    [TestCase("user:p)ss", "p)ss", TestName = "RedactUrls_redacts_userinfo_containing_a_close_paren")]
    [TestCase("user:p'ss", "p'ss", TestName = "RedactUrls_redacts_userinfo_containing_an_apostrophe")]
    public void RedactUrls_redacts_userinfo_containing_a_sub_delim(string userinfo, string secret)
    {
        var text = "failed to fetch " + Url(userinfo, "host.invalid/repo.git");

        var scrubbed = RepoContextSecretRedactor.RedactUrls(text);

        Assert.Multiple(() =>
        {
            Assert.That(scrubbed, Does.Not.Contain(secret));
            Assert.That(scrubbed, Does.Not.Contain(userinfo));
            Assert.That(scrubbed, Does.Contain("host.invalid/repo.git"));
            Assert.That(scrubbed, Does.Contain(RepoContextSecretRedactor.Placeholder));
        });
    }

    [Test]
    public void RedactUrls_redacts_both_urls_when_separated_by_a_comma()
    {
        var text = Url("a:1", "one.invalid") + "," + Url("b:2", "two.invalid");

        var scrubbed = RepoContextSecretRedactor.RedactUrls(text);

        Assert.Multiple(() =>
        {
            Assert.That(scrubbed, Does.Not.Contain("a:1"));
            Assert.That(scrubbed, Does.Not.Contain("b:2"));
            Assert.That(scrubbed, Does.Contain("one.invalid"));
            Assert.That(scrubbed, Does.Contain("two.invalid"));
        });
    }
}
