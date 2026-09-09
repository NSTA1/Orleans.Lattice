namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="GlobMatcher"/>: the minimal, dependency-free glob grammar
/// the bootstrap walker uses to honour include / exclude filters - single-segment
/// <c>*</c>, single-character <c>?</c>, recursive <c>**</c>, bare-basename
/// anchoring, and case-insensitive literal matching on <c>'/'</c>-separated paths.
/// </summary>
[TestFixture]
public sealed class GlobMatcherTests
{
    [Test]
    public void Star_matches_within_a_single_segment_only()
    {
        var matcher = GlobMatcher.Compile("src/*.cs");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("src/Program.cs"), Is.True);
            Assert.That(matcher.IsMatch("src/nested/Program.cs"), Is.False);
        });
    }

    [Test]
    public void Question_mark_matches_exactly_one_non_separator_character()
    {
        var matcher = GlobMatcher.Compile("a?.txt");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("ab.txt"), Is.True);
            Assert.That(matcher.IsMatch("a.txt"), Is.False);
            Assert.That(matcher.IsMatch("a/.txt"), Is.False);
        });
    }

    [Test]
    public void Double_star_matches_across_directory_boundaries()
    {
        var matcher = GlobMatcher.Compile("src/**/*.cs");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("src/Program.cs"), Is.True);
            Assert.That(matcher.IsMatch("src/a/b/Program.cs"), Is.True);
            Assert.That(matcher.IsMatch("other/Program.cs"), Is.False);
        });
    }

    [Test]
    public void A_bare_basename_pattern_matches_at_any_depth()
    {
        var matcher = GlobMatcher.Compile("*.cs");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("Program.cs"), Is.True);
            Assert.That(matcher.IsMatch("src/a/b/Program.cs"), Is.True);
            Assert.That(matcher.IsMatch("Program.txt"), Is.False);
        });
    }

    [Test]
    public void A_bare_literal_name_matches_that_file_at_any_depth()
    {
        var matcher = GlobMatcher.Compile("bin");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("bin"), Is.True);
            Assert.That(matcher.IsMatch("src/bin"), Is.True);
            Assert.That(matcher.IsMatch("src/binary"), Is.False);
        });
    }

    [Test]
    public void Matching_is_case_insensitive()
    {
        var matcher = GlobMatcher.Compile("*.CS");
        Assert.That(matcher.IsMatch("Program.cs"), Is.True);
    }

    [Test]
    public void Regex_metacharacters_in_the_pattern_are_matched_literally()
    {
        var matcher = GlobMatcher.Compile("src/a.b+c.cs");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("src/a.b+c.cs"), Is.True);
            Assert.That(matcher.IsMatch("src/axbxc.cs"), Is.False);
        });
    }

    [Test]
    public void Compile_rejects_a_null_pattern()
        => Assert.Throws<ArgumentNullException>(() => GlobMatcher.Compile(null!));

    [Test]
    public void IsMatch_rejects_a_null_path()
    {
        var matcher = GlobMatcher.Compile("*.cs");
        Assert.Throws<ArgumentNullException>(() => matcher.IsMatch(null!));
    }

    /// <summary>
    /// Regression: the include / exclude globs are caller-supplied over the wire,
    /// and the translation emits <c>(?:.*/)?</c> for every <c>**/</c> segment. Under
    /// the backtracking engine those ambiguous, mutually-overlapping quantifiers
    /// compose into exponential work on a path that does <em>not</em> match, so a
    /// short pattern paired with an ordinary repository path pinned the indexer
    /// thread indefinitely (CWE-1333). Measured against the unmodified source this
    /// case did not return within 30 seconds; the non-backtracking engine settles it
    /// in single-digit milliseconds.
    /// <para>
    /// The budget is deliberately generous - three orders of magnitude above the
    /// fixed behaviour - so the test asserts "bounded, not exponential" rather than
    /// a wall-clock figure that could flake on a loaded CI agent.
    /// </para>
    /// </summary>
    [Test]
    public void A_pattern_dense_in_recursive_wildcards_matches_in_bounded_time()
    {
        var pattern = string.Concat(Enumerable.Repeat("**/", 12)) + "x";
        var nonMatchingPath = string.Concat(Enumerable.Repeat("a/", 22));
        var matcher = GlobMatcher.Compile(pattern);

        bool? matched = null;
        var probe = Task.Run(() => matched = matcher.IsMatch(nonMatchingPath));

        Assert.That(
            probe.Wait(TimeSpan.FromSeconds(10)),
            Is.True,
            "A recursive-wildcard-dense glob must not backtrack exponentially on a non-matching path.");
        Assert.That(matched, Is.False);
    }

    [Test]
    public void A_pattern_dense_in_recursive_wildcards_still_matches_what_it_should()
    {
        // The bound above must not have been bought by changing the language the
        // grammar accepts: the same pattern still matches at any depth.
        var matcher = GlobMatcher.Compile(string.Concat(Enumerable.Repeat("**/", 12)) + "x");

        Assert.Multiple(() =>
        {
            Assert.That(matcher.IsMatch("x"), Is.True);
            Assert.That(matcher.IsMatch("a/b/c/x"), Is.True);
            Assert.That(matcher.IsMatch("a/b/c/y"), Is.False);
        });
    }
}
