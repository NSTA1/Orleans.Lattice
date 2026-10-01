namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// The grammar edges of <see cref="GitignoreScope"/>: the arms of the
/// <c>gitignore</c>(5) subset that the ordinary-usage fixture alongside this one
/// never reaches, because no realistic <c>.gitignore</c> line exercises them.
/// <para>
/// WHY THEY ARE WORTH PINNING ANYWAY. Every one of these arms exists to stop a
/// pattern being mis-translated into a regular expression, and a mis-translation
/// does not fail loudly - it silently ignores the wrong set of files. An
/// over-matching rule drops real source out of the index while every health
/// signal stays green, which is precisely the class of defect that is only ever
/// found by someone asking why a file they can see is not searchable. A
/// degenerate line that should parse to nothing and instead parses to a rule is
/// the same failure from the other side.
/// </para>
/// <para>
/// The entry-only seam <see cref="GitignoreScope.IsEntryIgnored"/> is covered
/// here rather than beside <see cref="GitignoreScope.IsIgnored"/> because it is a
/// different contract: it classifies the entry alone, on the caller's guarantee
/// that no ancestor is ignored, and the tree walk relies on that distinction to
/// prune rather than re-classify. A fixture that only drove the hierarchical
/// entry point would leave the one the walk actually calls untested.
/// </para>
/// </summary>
public sealed partial class GitignoreScopeTests
{
    // -- the entry-only seam -----------------------------------------

    [Test]
    public void The_empty_scope_ignores_nothing_at_the_entry_only_seam()
        => Assert.That(GitignoreScope.Empty.IsEntryIgnored("any/path.cs", isDirectory: false), Is.False);

    [Test]
    public void The_entry_only_seam_classifies_the_entry_without_consulting_its_ancestors()
    {
        // This is the whole difference between the two seams. The walk prunes an
        // ignored directory instead of descending it, so by the time it classifies
        // a file no ancestor can be ignored and re-checking them would be wasted
        // work. A caller without that guarantee must use IsIgnored, which is why
        // the two must not be allowed to drift into the same behaviour.
        var scope = GitignoreScope.Empty.Add(string.Empty, "bin/\n");

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.IsEntryIgnored("bin/app.dll", isDirectory: false), Is.False,
                "the entry itself does not match 'bin/'; only its ancestor does");
            Assert.That(
                scope.IsIgnored("bin/app.dll", isDirectory: false), Is.True,
                "the hierarchical seam classifies the ancestor first, and the ancestor decides");
        });
    }

    [Test]
    public void The_entry_only_seam_rejects_a_null_path()
        => Assert.Throws<ArgumentNullException>(
            () => GitignoreScope.Empty.IsEntryIgnored(null!, isDirectory: false));

    [Test]
    public void A_layer_does_not_apply_to_a_path_outside_its_base_directory()
    {
        var scope = GitignoreScope.Empty.Add("sub", "*.log\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("sub/a.log", isDirectory: false), Is.True);
            Assert.That(
                scope.IsIgnored("other/a.log", isDirectory: false), Is.False,
                "a nested .gitignore governs its own directory, never a sibling");
            Assert.That(
                scope.IsIgnored("subtle/a.log", isDirectory: false), Is.False,
                "and the base directory is matched as a whole segment, not as a string prefix");
        });
    }

    // -- degenerate lines that must parse to no rule -----------------

    [TestCase("!\n", TestName = "A bare negation marker is not a rule")]
    [TestCase("/\n", TestName = "A bare slash is not a rule")]
    [TestCase("//\n", TestName = "An anchored empty pattern is not a rule")]
    [TestCase("!/\n", TestName = "A negated bare slash is not a rule")]
    public void A_degenerate_line_contributes_no_rule(string content)
    {
        // Each of these reduces to the empty pattern as it is peeled apart. A rule
        // compiled from one would translate to a bare anchor and so match every
        // path - an empty .gitignore line that silently ignored the entire
        // repository, with nothing to attribute the emptiness to.
        var scope = GitignoreScope.Empty.Add(string.Empty, content);

        Assert.Multiple(() =>
        {
            Assert.That(
                ReferenceEquals(scope, GitignoreScope.Empty), Is.True,
                "a file that contributes no effective rule must add no layer at all");
            Assert.That(scope.IsIgnored("anything.cs", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_escaped_bang_is_a_literal_pattern_rather_than_a_negation()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "\\!important\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("!important", isDirectory: false), Is.True);
            Assert.That(
                scope.IsIgnored("important", isDirectory: false), Is.False,
                "the backslash makes the '!' literal; it must not also be consumed as a negation");
        });
    }

    [Test]
    public void Unescaped_trailing_spaces_are_stripped_from_a_pattern()
    {
        // gitignore(5): trailing spaces are ignored unless quoted with a backslash.
        // The escaped case is pinned by the fixture alongside this one; this is the
        // ordinary case, which is what a text editor leaves behind.
        var scope = GitignoreScope.Empty.Add(string.Empty, "notes.txt   \n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("notes.txt", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("notes.txt ", isDirectory: false), Is.False);
        });
    }

    // -- wildcard translation ----------------------------------------

    [Test]
    public void A_trailing_double_star_crosses_directory_boundaries()
    {
        // '**' not followed by '/' translates to '.*', which spans separators,
        // where a single '*' translates to '[^/]*', which does not. Collapsing the
        // two would make every '**' tail behave like a single '*' and quietly stop
        // pruning the subtree the author meant to exclude.
        var scope = GitignoreScope.Empty.Add(string.Empty, "logs**\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsEntryIgnored("logs", isDirectory: false), Is.True);
            Assert.That(scope.IsEntryIgnored("logsarchive", isDirectory: false), Is.True);
            Assert.That(
                scope.IsEntryIgnored("logsarchive/2026/a.txt", isDirectory: false), Is.True,
                "'**' spans separators even as a tail");
            Assert.That(scope.IsEntryIgnored("log", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_single_star_tail_stops_at_a_directory_boundary()
    {
        // The control for the test above: same shape, one less star.
        var scope = GitignoreScope.Empty.Add(string.Empty, "logs*\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsEntryIgnored("logsarchive", isDirectory: false), Is.True);
            Assert.That(scope.IsEntryIgnored("logsarchive/2026/a.txt", isDirectory: false), Is.False);
        });
    }

    // -- bracket expressions -----------------------------------------

    [Test]
    public void A_closing_bracket_as_the_first_member_is_a_literal()
    {
        // POSIX bracket grammar: ']' first is a member, not the terminator. Reading
        // it as the terminator would translate '[]]' to an empty class, which
        // matches nothing and silently disables the rule.
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[]]b\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a]b", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("ab", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_caret_negates_a_character_class_as_a_bang_does()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "x[^0-9]y\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("xay", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("x5y", isDirectory: false), Is.False);
            Assert.That(
                scope.IsIgnored("x/y", isDirectory: false), Is.False,
                "a negated class still never crosses a directory boundary");
        });
    }

    [Test]
    public void A_caret_that_is_not_the_first_member_is_an_ordinary_literal()
    {
        // Emitted escaped, so it cannot be re-read as a negation marker by the
        // regular-expression engine and invert the whole class.
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[b^]c\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a^c", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("abc", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("adc", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_opening_bracket_inside_a_class_is_an_ordinary_literal()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[b[]c\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a[c", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("abc", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("adc", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void A_backslash_inside_a_class_is_an_ordinary_literal()
    {
        // Left unescaped it would start an escape sequence in the emitted regular
        // expression and consume the class terminator.
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[\\]b\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a\\b", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("ab", isDirectory: false), Is.False);
        });
    }

    [Test]
    public void An_unterminated_bracket_after_a_negation_marker_is_a_literal()
    {
        // The terminator search skips a leading negation marker, so this one runs
        // off the end of the pattern and the '[' falls back to a literal.
        var scope = GitignoreScope.Empty.Add(string.Empty, "a[!b\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a[!b", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("ab", isDirectory: false), Is.False);
        });
    }
}
