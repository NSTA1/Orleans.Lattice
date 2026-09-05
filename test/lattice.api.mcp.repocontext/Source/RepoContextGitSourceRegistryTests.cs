namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// Tests for the opt-in git-source configuration surface. The decisive property is
/// that the feature is inert until an operator explicitly declares a repository:
/// with no configuration the registry is empty, every repository stays mounted, and
/// nothing about the default deployment changes.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextGitSourceRegistryTests
{
    private const string RepoId = "my-repo";

    private readonly List<string> _setVariables = [];

    [TearDown]
    public void ClearEnvironment()
    {
        foreach (var name in _setVariables)
        {
            Environment.SetEnvironmentVariable(name, null);
        }

        _setVariables.Clear();
    }

    private void SetVariable(string name, string? value)
    {
        _setVariables.Add(name);
        Environment.SetEnvironmentVariable(name, value);
    }

    private void SetSetting(string setting, string? value) =>
        SetVariable(RepoContextGitSourceRegistry.VariableName(RepoId, setting), value);

    [Test]
    public void Empty_registry_leaves_every_repository_mounted()
    {
        var registry = RepoContextGitSourceRegistry.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(registry.IsEmpty, Is.True);
            Assert.That(registry.Sources, Is.Empty);
            Assert.That(registry.IsGitSourced(RepoId), Is.False);
            Assert.That(registry.Find(RepoId), Is.Null);
            Assert.That(registry.StagingRoot, Is.Not.Empty);
        });
    }

    [Test]
    public void Constructor_rejects_null_sources_and_a_blank_staging_root()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextGitSourceRegistry(null!, "root"), Throws.ArgumentNullException);
            Assert.That(() => new RepoContextGitSourceRegistry([], " "), Throws.ArgumentException);
        });
    }

    [Test]
    public void Lookups_reject_a_null_repository_id()
    {
        var registry = RepoContextGitSourceRegistry.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(() => registry.IsGitSourced(null!), Throws.ArgumentNullException);
            Assert.That(() => registry.Find(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Constructor_keeps_the_first_entry_for_a_duplicated_repository_id()
    {
        var registry = new RepoContextGitSourceRegistry(
            [
                new RepoContextGitSourceOptions { RepoId = RepoId, RemoteUrl = "first" },
                new RepoContextGitSourceOptions { RepoId = RepoId, RemoteUrl = "second" },
            ],
            Path.GetTempPath());

        Assert.That(registry.Find(RepoId)!.RemoteUrl, Is.EqualTo("first"),
            "A later duplicate must not silently redirect a declared repository at another remote.");
    }

    [Test]
    public void VariableName_folds_a_repository_id_into_an_upper_case_identifier()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextGitSourceRegistry.VariableName("my-repo", "URL"),
                Is.EqualTo("LATTICE_REPOCONTEXT_GIT_MY_REPO_URL"));
            Assert.That(
                RepoContextGitSourceRegistry.VariableName("a.b/c", "TOKEN"),
                Is.EqualTo("LATTICE_REPOCONTEXT_GIT_A_B_C_TOKEN"));
        });
    }

    [Test]
    public void VariableName_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextGitSourceRegistry.VariableName(null!, "URL"), Throws.ArgumentNullException);
            Assert.That(() => RepoContextGitSourceRegistry.VariableName("r", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void FromEnvironment_is_empty_when_no_repository_is_declared()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, null);

        Assert.That(RepoContextGitSourceRegistry.FromEnvironment().IsEmpty, Is.True,
            "The mounted workspace stays the default until an operator opts in.");
    }

    [Test]
    public void FromEnvironment_reads_a_declared_repository_with_its_defaults()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);
        SetSetting("URL", "https://git.example.invalid/acme.git");

        var source = RepoContextGitSourceRegistry.FromEnvironment().Find(RepoId);

        Assert.That(source, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(source!.RemoteUrl, Is.EqualTo("https://git.example.invalid/acme.git"));
            Assert.That(source.Reference, Is.EqualTo(RepoContextGitReference.DefaultReference));
            Assert.That(source.Depth, Is.EqualTo(1), "A shallow fetch keeps the hub's working copy small.");
            Assert.That(source.AuthMode, Is.EqualTo(RepoContextGitAuthMode.Token));
            Assert.That(source.ExcludeBinary, Is.True);
            Assert.That(source.RefreshInterval, Is.EqualTo(RepoContextGitSourceOptions.DefaultRefreshInterval));
            Assert.That(source.FetchTimeout, Is.EqualTo(RepoContextGitSourceOptions.DefaultFetchTimeout));
        });
    }

    [Test]
    public void FromEnvironment_registers_a_declared_repository_even_without_a_url()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);

        var registry = RepoContextGitSourceRegistry.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(registry.IsGitSourced(RepoId), Is.True,
                "A misconfigured declaration must fail closed, never degrade to a mounted walk.");
            Assert.That(registry.Find(RepoId)!.RemoteUrl, Is.Empty);
        });
    }

    [Test]
    public void FromEnvironment_splits_the_repository_list_on_both_separators()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, " one ; two , one ");
        SetVariable(RepoContextGitSourceRegistry.VariableName("one", "URL"), "u1");
        SetVariable(RepoContextGitSourceRegistry.VariableName("two", "URL"), "u2");

        var registry = RepoContextGitSourceRegistry.FromEnvironment();

        Assert.That(registry.Sources, Has.Count.EqualTo(2), "A repeated id is declared once.");
    }

    [Test]
    public void FromEnvironment_clamps_and_falls_back_on_malformed_numbers()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);
        SetSetting("URL", "u");
        SetSetting("DEPTH", "not-a-number");
        SetSetting("REFRESH_SECONDS", "1");
        SetSetting("FETCH_TIMEOUT_SECONDS", "999999");

        var source = RepoContextGitSourceRegistry.FromEnvironment().Find(RepoId)!;

        Assert.Multiple(() =>
        {
            Assert.That(source.Depth, Is.EqualTo(1), "A malformed depth falls back rather than failing startup.");
            Assert.That(source.RefreshInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(source.FetchTimeout, Is.EqualTo(TimeSpan.FromSeconds(3_600)));
        });
    }

    [Test]
    public void FromEnvironment_reads_the_remaining_per_repository_settings()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);
        SetSetting("URL", "u");
        SetSetting("REF", "release/v1");
        SetSetting("AUTH", "Anonymous");
        SetSetting("INCLUDE", "src/**;docs/**");
        SetSetting("EXCLUDE", "**/bin/**");
        SetSetting("EXCLUDE_BINARY", "false");

        var source = RepoContextGitSourceRegistry.FromEnvironment().Find(RepoId)!;

        Assert.Multiple(() =>
        {
            Assert.That(source.Reference, Is.EqualTo("release/v1"));
            Assert.That(source.AuthMode, Is.EqualTo(RepoContextGitAuthMode.Anonymous));
            Assert.That(source.IncludeGlobs, Is.EqualTo(new[] { "src/**", "docs/**" }));
            Assert.That(source.ExcludeGlobs, Is.EqualTo(new[] { "**/bin/**" }));
            Assert.That(source.ExcludeBinary, Is.False);
        });
    }

    [Test]
    public void FromEnvironment_honours_a_configured_staging_root()
    {
        var root = Path.Combine(Path.GetTempPath(), "lattice-staging-override");
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);
        SetSetting("URL", "u");
        SetVariable(RepoContextGitSourceRegistry.StagingRootVariable, root);

        Assert.That(
            RepoContextGitSourceRegistry.FromEnvironment().StagingRoot, Is.EqualTo(Path.GetFullPath(root)));
    }

    [Test]
    public void FromEnvironment_resolves_a_per_repository_token_and_isolates_it()
    {
        SetVariable(RepoContextGitSourceRegistry.ReposVariable, "one;two");
        SetVariable(RepoContextGitSourceRegistry.VariableName("one", "URL"), "u1");
        SetVariable(RepoContextGitSourceRegistry.VariableName("two", "URL"), "u2");
        SetVariable(RepoContextGitSourceRegistry.VariableName("one", "TOKEN"), "token-one");

        var registry = RepoContextGitSourceRegistry.FromEnvironment();
        var provider = RepoContextEnvironmentGitCredentialProvider.FromEnvironment(registry);

        var first = provider.ResolveAsync(registry.Find("one")!, CancellationToken.None).AsTask().Result;
        var second = provider.ResolveAsync(registry.Find("two")!, CancellationToken.None).AsTask().Result;

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(first!.Secret, Is.EqualTo("token-one"));
            Assert.That(second, Is.Null,
                "One repository's token is never presented to another repository's remote.");
        });
    }
}
