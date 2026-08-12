namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="GitignoreScope"/>: the dependency-free, hierarchical
/// <c>.gitignore</c> matcher. They pin the pattern grammar (anchoring, directory
/// only, wildcards, negation) and the layered precedence a nested file has over a
/// shallower one, independently of the filesystem walk that consumes it.
/// </summary>
[TestFixture]
public sealed class GitignoreScopeTests
{
    [Test]
    public void Empty_scope_ignores_nothing()
    {
        Assert.That(GitignoreScope.Empty.IsIgnored("any/path.cs", isDirectory: false), Is.False);
    }

    [Test]
    public void A_bare_pattern_matches_at_any_depth()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "*.log\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a.log", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("deep/nested/b.log", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("a.cs", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_leading_slash_anchors_to_the_gitignore_directory()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "/build\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("build", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("src/build", isDirectory: true), Is.False);
        });
    }

    [Test]
    public void An_interior_slash_anchors_the_pattern()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "a/b\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a/b", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("x/a/b", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_directory_only_pattern_does_not_match_a_file()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "bin/\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("bin", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("bin", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_ignored_directory_carries_its_subtree_at_the_file_seam()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "node_modules/\n");

        // The directory itself matches; a nested file also matches so a caller that
        // classifies files (rather than pruning) still excludes the subtree.
        Assert.That(scope.IsIgnored("node_modules/pkg/index.js", isDirectory: false), Is.True);
    }

    [Test]
    public void A_double_star_matches_across_directory_boundaries()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "a/**/z\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a/z", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("a/b/z", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("a/b/c/z", isDirectory: false), Is.True);
        });
    }

    [Test]
    public void The_last_matching_rule_in_a_file_wins()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "*.log\n!keep.log\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("keep.log", isDirectory: false), Is.False);
            Assert.That(scope.IsIgnored("other.log", isDirectory: false), Is.True);
        });
    }

    [Test]
    public void A_deeper_layer_overrides_a_shallower_one()
    {
        var scope = GitignoreScope.Empty
            .Add(string.Empty, "*.log\n")
            .Add("sub", "!keep.log\n");

        Assert.Multiple(() =>
        {
            // The nested re-include only applies within its own base directory.
            Assert.That(scope.IsIgnored("sub/keep.log", isDirectory: false), Is.False);
            Assert.That(scope.IsIgnored("sub/other.log", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("keep.log", isDirectory: false), Is.True);
        });
    }

    [Test]
    public void Comments_blank_lines_and_whitespace_are_skipped()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "# comment\n\n   \n*.tmp\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a.tmp", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("comment", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_escaped_hash_is_a_literal_pattern()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "\\#notacomment\n");

        Assert.That(scope.IsIgnored("#notacomment", isDirectory: false), Is.True);
    }

    [Test]
    public void An_empty_file_adds_no_layer()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "# only a comment\n");

        Assert.That(ReferenceEquals(scope, GitignoreScope.Empty), Is.True);
    }

    [Test]
    public void A_question_mark_matches_a_single_non_separator_character()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "file?.cs\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("file1.cs", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("file.cs", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_single_star_does_not_cross_a_directory_separator()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "a*b\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("axxb", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("ax/xb", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_plain_directory_name_ignores_the_directory_and_its_subtree()
    {
        // No trailing slash, so the pattern matches a file or a directory of that
        // name at any depth, and everything nested beneath the directory.
        var scope = GitignoreScope.Empty.Add(string.Empty, "dist\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("dist", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("dist", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("dist/app.js", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("src/dist/app.js", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("distant.cs", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_character_class_matches_any_listed_character()
    {
        // The ubiquitous Visual Studio pattern: [Bb]in/ must prune bin and Bin.
        var scope = GitignoreScope.Empty.Add(string.Empty, "[Bb]in/\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("bin", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("Bin", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("src/obj/Bin", isDirectory: true), Is.True);
            Assert.That(scope.IsIgnored("bin/Release/app.dll", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("din", isDirectory: true), Is.False);
        });
    }

    [Test]
    public void A_character_class_supports_ranges()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "file[0-9].cs\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("file3.cs", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("fileZ.cs", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_negated_character_class_excludes_listed_characters_and_the_separator()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "x[!0-9]y\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("xay", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("x5y", isDirectory: false), Is.False);
            // The class never matches the path separator.
            Assert.That(scope.IsIgnored("x/y", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_unterminated_bracket_is_a_literal()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[b\n");

        Assert.That(scope.IsIgnored("a[b", isDirectory: false), Is.True);
    }

    [Test]
    public void Add_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => GitignoreScope.Empty.Add(null!, "x"));
            Assert.Throws<ArgumentNullException>(() => GitignoreScope.Empty.Add(string.Empty, null!));
            Assert.Throws<ArgumentNullException>(() => GitignoreScope.Empty.IsIgnored(null!, false));
        });
    }
}
