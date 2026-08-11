namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextKeys"/>: the key grammar builders, the
/// range-scan prefixes, the percent-encoding round-trips, and the
/// <see cref="RepoContextKeys.TryParse(string, out RepoContextKey)"/> parser -
/// including ordered-range and prefix-scan behaviour and hostile inputs.
/// </summary>
[TestFixture]
public sealed class RepoContextKeysTests
{
    [Test]
    public void Repo_builds_the_root_key()
        => Assert.That(RepoContextKeys.Repo("acme"), Is.EqualTo("repo/acme"));

    [Test]
    public void File_builds_the_hierarchical_key_preserving_slashes()
        => Assert.That(RepoContextKeys.File("acme", "src/app/main.cs"),
            Is.EqualTo("repo/acme/file/src/app/main.cs"));

    [Test]
    public void Package_builds_the_hierarchical_key()
        => Assert.That(RepoContextKeys.Package("acme", "src/app"),
            Is.EqualTo("repo/acme/pkg/src/app"));

    [Test]
    public void Symbol_builds_the_key_preserving_dotted_names()
        => Assert.That(RepoContextKeys.Symbol("acme", "Acme.App.Program.Main"),
            Is.EqualTo("repo/acme/symbol/Acme.App.Program.Main"));

    [Test]
    public void Memory_builds_the_two_component_key()
        => Assert.That(RepoContextKeys.Memory("acme", "decisions", "0001"),
            Is.EqualTo("repo/acme/mem/decisions/0001"));

    [Test]
    public void Repo_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.Repo("acme");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.Repo));
            Assert.That(parsed.RepoId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void File_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.File("acme", "src/app/main.cs");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.File));
            Assert.That(parsed.RepoId, Is.EqualTo("acme"));
            Assert.That(parsed.Path, Is.EqualTo("src/app/main.cs"));
        });
    }

    [Test]
    public void Package_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.Package("acme", "src/app");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.Package));
            Assert.That(parsed.Path, Is.EqualTo("src/app"));
        });
    }

    [Test]
    public void Symbol_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.Symbol("acme", "Acme.App.Program.Main");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.Symbol));
            Assert.That(parsed.FullyQualifiedName, Is.EqualTo("Acme.App.Program.Main"));
        });
    }

    [Test]
    public void Memory_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.Memory("acme", "decisions", "0001");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.Memory));
            Assert.That(parsed.Topic, Is.EqualTo("decisions"));
            Assert.That(parsed.Id, Is.EqualTo("0001"));
        });
    }

    [Test]
    public void Reserved_characters_in_opaque_components_round_trip()
    {
        var key = RepoContextKeys.Memory("acme/team", "a/b", "id%2F1");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.RepoId, Is.EqualTo("acme/team"));
            Assert.That(parsed.Topic, Is.EqualTo("a/b"));
            Assert.That(parsed.Id, Is.EqualTo("id%2F1"));
        });
    }

    [Test]
    public void Percent_in_a_path_round_trips_while_slashes_are_preserved()
    {
        var key = RepoContextKeys.File("acme", "src/a%b/c.cs");
        // The '%' is escaped so the raw key is unambiguous, but the directory
        // separators stay literal so the subtree remains a contiguous range.
        Assert.That(key, Is.EqualTo("repo/acme/file/src/a%25b/c.cs"));
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.That(parsed.Path, Is.EqualTo("src/a%b/c.cs"));
    }

    [Test]
    public void Encoded_repo_id_has_no_stray_separator()
    {
        var key = RepoContextKeys.File("a/b", "x.cs");
        Assert.That(key, Is.EqualTo("repo/a%2Fb/file/x.cs"));
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.RepoId, Is.EqualTo("a/b"));
            Assert.That(parsed.Path, Is.EqualTo("x.cs"));
        });
    }

    [Test]
    public void Files_under_a_directory_share_a_scan_prefix()
    {
        var prefix = RepoContextKeys.FilesUnderPrefix("acme", "src/app");
        var inside = RepoContextKeys.File("acme", "src/app/main.cs");
        var outside = RepoContextKeys.File("acme", "src/other/main.cs");

        Assert.Multiple(() =>
        {
            Assert.That(prefix, Is.EqualTo("repo/acme/file/src/app/"));
            Assert.That(inside, Does.StartWith(prefix));
            Assert.That(outside, Does.Not.StartWith(prefix));
        });
    }

    [Test]
    public void FilesUnderPrefix_normalises_a_trailing_separator()
        => Assert.That(RepoContextKeys.FilesUnderPrefix("acme", "src/app/"),
            Is.EqualTo("repo/acme/file/src/app/"));

    [Test]
    public void FilesUnderPrefix_with_empty_directory_is_the_files_prefix()
        => Assert.That(RepoContextKeys.FilesUnderPrefix("acme", ""),
            Is.EqualTo(RepoContextKeys.FilesPrefix("acme")));

    [Test]
    public void Every_family_prefix_is_a_prefix_of_its_key()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextKeys.File("acme", "x.cs"), Does.StartWith(RepoContextKeys.FilesPrefix("acme")));
            Assert.That(RepoContextKeys.Package("acme", "x"), Does.StartWith(RepoContextKeys.PackagesPrefix("acme")));
            Assert.That(RepoContextKeys.Symbol("acme", "X"), Does.StartWith(RepoContextKeys.SymbolsPrefix("acme")));
            Assert.That(RepoContextKeys.Memory("acme", "t", "1"), Does.StartWith(RepoContextKeys.MemoryPrefix("acme")));
            Assert.That(RepoContextKeys.Memory("acme", "t", "1"), Does.StartWith(RepoContextKeys.MemoryTopicPrefix("acme", "t")));
        });
    }

    [Test]
    public void Keys_sort_so_a_repository_subtree_is_a_contiguous_range()
    {
        var keys = new[]
        {
            RepoContextKeys.File("acme", "b.cs"),
            RepoContextKeys.File("other", "a.cs"),
            RepoContextKeys.File("acme", "a.cs"),
        };

        var sorted = keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        // Both acme keys are adjacent and precede the "other" repository key.
        Assert.Multiple(() =>
        {
            Assert.That(sorted[0], Does.StartWith("repo/acme/"));
            Assert.That(sorted[1], Does.StartWith("repo/acme/"));
            Assert.That(sorted[2], Does.StartWith("repo/other/"));
        });
    }

    [TestCase("")]
    [TestCase("notrepo/acme/file/x")]
    [TestCase("repo/")]
    [TestCase("repo/acme/")]
    [TestCase("repo/acme/file/")]
    [TestCase("repo/acme/bogus/x")]
    [TestCase("repo/acme/mem/topic")]
    [TestCase("repo/acme/mem/topic/id/extra")]
    public void TryParse_rejects_malformed_keys(string key)
    {
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.False);
        Assert.That(parsed, Is.EqualTo(default(RepoContextKey)));
    }

    [Test]
    public void Builders_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextKeys.Repo(null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextKeys.File("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextKeys.Symbol("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextKeys.Memory("acme", "t", null!), Throws.ArgumentNullException);
        });
    }
}
