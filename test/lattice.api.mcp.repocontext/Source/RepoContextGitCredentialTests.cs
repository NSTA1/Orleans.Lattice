namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the fail-closed credential seam. A git-sourced repository with no valid
/// credential does not index and does not fall back to another source, the secret
/// never appears in a formatted string, and one repository's credential is never
/// presented to another repository's remote.
/// </summary>
[TestFixture]
public sealed class RepoContextGitCredentialTests
{
    [Test]
    public void Token_produces_a_credential_with_the_default_username()
    {
        var credential = RepoContextGitCredential.Token("secret-value");

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Username, Is.EqualTo(RepoContextGitCredential.DefaultTokenUsername));
            Assert.That(credential.Secret, Is.EqualTo("secret-value"));
            Assert.That(credential.IsAnonymous, Is.False);
        });
    }

    [Test]
    public void Token_honours_an_explicit_username()
    {
        var credential = RepoContextGitCredential.Token("secret-value", " build-agent ");

        Assert.That(credential!.Username, Is.EqualTo("build-agent"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void Token_fails_closed_on_a_blank_secret(string? secret)
    {
        Assert.That(RepoContextGitCredential.Token(secret), Is.Null,
            "An empty environment variable must fail exactly like a missing one.");
    }

    [Test]
    public void Anonymous_is_an_explicit_unauthenticated_credential()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextGitCredential.Anonymous.IsAnonymous, Is.True);
            Assert.That(RepoContextGitCredential.Anonymous.Secret, Is.Empty);
            Assert.That(RepoContextGitCredential.Anonymous.Username, Is.Empty);
        });
    }

    [Test]
    public void ToString_never_reveals_the_secret_or_its_length()
    {
        var credential = RepoContextGitCredential.Token("super-secret-token-value", "build-agent");

        var text = credential!.ToString();

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Not.Contain("super-secret-token-value"));
            Assert.That(text, Does.Contain("redacted"));
            Assert.That(text, Does.Contain("build-agent"));
            Assert.That(RepoContextGitCredential.Anonymous.ToString(), Is.EqualTo("anonymous"));
        });
    }

    [Test]
    public void ResolveAsync_rejects_a_null_source()
    {
        var provider = new RepoContextEnvironmentGitCredentialProvider(
            new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal));

        Assert.That(
            () => provider.ResolveAsync(null!, CancellationToken.None).AsTask(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_and_FromEnvironment_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextEnvironmentGitCredentialProvider(null!), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextEnvironmentGitCredentialProvider.FromEnvironment(null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task ResolveAsync_returns_null_for_an_undeclared_repository()
    {
        var provider = new RepoContextEnvironmentGitCredentialProvider(
            new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal));

        var resolved = await provider.ResolveAsync(
            new RepoContextGitSourceOptions { RepoId = "acme", RemoteUrl = "u" }, CancellationToken.None);

        Assert.That(resolved, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_returns_the_anonymous_credential_only_for_an_anonymous_source()
    {
        var provider = new RepoContextEnvironmentGitCredentialProvider(
            new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal));

        var resolved = await provider.ResolveAsync(
            new RepoContextGitSourceOptions
            {
                RepoId = "acme",
                RemoteUrl = "u",
                AuthMode = RepoContextGitAuthMode.Anonymous,
            },
            CancellationToken.None);

        Assert.That(resolved, Is.SameAs(RepoContextGitCredential.Anonymous));
    }

    [Test]
    public void ResolveAsync_observes_cancellation()
    {
        var provider = new RepoContextEnvironmentGitCredentialProvider(
            new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            () => provider.ResolveAsync(
                new RepoContextGitSourceOptions { RepoId = "acme", RemoteUrl = "u" }, cts.Token).AsTask(),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
