namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Regression tests for the fail-open direction of <see cref="GitignoreScope"/>.
/// A <c>.gitignore</c> rule is a <em>deny-list</em> - a match removes the entry
/// from the walk - so unlike an allow-list, failing to match is failing
/// <em>open</em>: the ignored entry is indexed anyway and becomes retrievable
/// through the repository-context tools. These pin the two ways a path could
/// previously dodge a rule it plainly matched.
/// </summary>
public sealed partial class GitignoreScopeTests
{
    /// <summary>
    /// The translation emits <c>(?:.*/)?</c> for a non-anchored pattern and for
    /// every <c>**/</c> segment. Without <c>RegexOptions.Singleline</c>
    /// <c>.</c> does not match a line feed, so that prefix could not traverse a
    /// directory name containing one. A POSIX segment may hold any byte but
    /// <c>'/'</c> and NUL, so a single line feed anywhere in the prefix defeated
    /// every rule in the file.
    /// </summary>
    [TestCase("*.env\n", "app\nx/creds.env", TestName = "IsIgnored_matches_a_newline_bearing_prefix_for_a_bare_pattern")]
    [TestCase("a/**/z\n", "a/mid\ndle/z", TestName = "IsIgnored_matches_a_newline_bearing_segment_for_a_double_star")]
    public void IsIgnored_is_not_defeated_by_a_line_feed_in_a_path_segment(string content, string path)
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, content);

        Assert.That(
            scope.IsIgnored(path, isDirectory: false),
            Is.True,
            "A line feed in a path segment must not let an entry dodge a .gitignore rule.");
    }

    /// <summary>
    /// A directory rule carries its whole subtree at the file seam, so a line feed
    /// in the prefix must not readmit a file under an ignored directory.
    /// </summary>
    [Test]
    public void IsIgnored_matches_a_subtree_file_under_a_newline_bearing_prefix()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "secrets/\n");

        Assert.That(scope.IsIgnored("proj\nx/secrets/key.pem", isDirectory: false), Is.True);
    }

    /// <summary>
    /// The rule is terminated with <c>\z</c> rather than <c>$</c>, which also
    /// matches immediately before a line feed that ends the input. Under <c>$</c> a
    /// path was tested as a different string than the one being filtered, so a rule
    /// matched an entry it did not name.
    /// </summary>
    [Test]
    public void IsIgnored_anchors_at_the_true_end_of_the_path()
    {
        var scope = GitignoreScope.Empty.Add(string.Empty, "*.log\n");

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsIgnored("a.log", isDirectory: false), Is.True);
            Assert.That(scope.IsIgnored("a.log\nb.cs", isDirectory: false), Is.False);
        });
    }

    /// <summary>
    /// The newline fix must not have widened the grammar: a single <c>*</c> still
    /// stops at a segment boundary and an anchored rule still does not float.
    /// </summary>
    [Test]
    public void Matching_across_line_feeds_does_not_widen_the_rule_grammar()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                GitignoreScope.Empty.Add(string.Empty, "a/b\n").IsIgnored("x/a/b", isDirectory: false),
                Is.False);
            Assert.That(
                GitignoreScope.Empty.Add(string.Empty, "/build\n").IsIgnored("src/build", isDirectory: true),
                Is.False);
            Assert.That(
                GitignoreScope.Empty.Add(string.Empty, "*.log\n").IsIgnored("a.cs", isDirectory: false),
                Is.False);
        });
    }
}
