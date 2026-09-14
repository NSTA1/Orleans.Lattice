namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Unit tests for <see cref="RepoContextIndexedRootReporter.IsIdRootMismatch"/>, the
/// predicate behind the startup warning that a repository id disagrees with the root it
/// was indexed from.
/// <para>
/// This is the detector for issue #2617, in which a git worktree was indexed under the
/// BASE repository's id and no observable surface distinguished it from a correct index.
/// The predicate is deliberately pure and takes both values as parameters, so the failing
/// direction can be exercised here rather than reasoned about - a detector that has never
/// been shown to fire is not known to be a detector.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextIndexedRootMismatchTests
{
    /// <summary>
    /// The incident's exact values. Kept as constants because the whole point of this
    /// fixture is that these two strings were simultaneously true of one repository and
    /// nothing anywhere reported the pair.
    /// </summary>
    private const string IncidentRepoId = "lattice";

    private const string IncidentRoot = "/workspace/bucket4-merge";

    /// <summary>
    /// The positive control. Without this arm the fixture could pass with a predicate
    /// that returns false unconditionally, which is precisely the failure it exists to
    /// prevent: a warning that never fires reads identically to a healthy deployment.
    /// </summary>
    [Test]
    public void Flags_the_issue_2617_state_where_a_worktree_is_indexed_under_the_base_id()
    {
        Assert.That(
            RepoContextIndexedRootReporter.IsIdRootMismatch(IncidentRepoId, IncidentRoot),
            Is.True,
            "The id read 'lattice' while every record described '/workspace/bucket4-merge'. "
            + "If this arm does not fire, the detector is decoration.");
    }

    [Test]
    public void Accepts_a_root_whose_final_segment_is_the_repository_id()
    {
        Assert.That(
            RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", "/workspace/lattice"),
            Is.False,
            "The ordinary correct deployment must not warn, or the warning is noise.");
    }

    [Test]
    public void Tolerates_a_trailing_separator_on_the_root()
    {
        Assert.That(
            RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", "/workspace/lattice/"),
            Is.False,
            "A trailing separator is a spelling of the same path, not a different tree.");
    }

    [Test]
    public void Reads_the_final_segment_of_a_windows_style_root()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", @"C:\dev\lattice"),
                Is.False);
            Assert.That(
                RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", @"C:\dev\worktrees\umbrella"),
                Is.True);
        });
    }

    /// <summary>
    /// A never-indexed repository (or one whose index was reset, which clears the durable
    /// request the root is read from) is a distinct state with its own reporting. Calling
    /// it a mismatch would fire the warning on every reset, training an operator to ignore
    /// the one signal that separates a healthy index from an index of the wrong tree.
    /// </summary>
    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void Does_not_flag_a_repository_with_no_indexed_root(string? indexedRoot)
    {
        Assert.That(
            RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", indexedRoot),
            Is.False,
            "'never indexed' is not evidence of a mismatch.");
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void Does_not_flag_a_blank_repository_id(string? repoId)
    {
        Assert.That(
            RepoContextIndexedRootReporter.IsIdRootMismatch(repoId!, "/workspace/lattice"),
            Is.False);
    }

    /// <summary>
    /// A root that is nothing but separators has no final segment to compare, so there is
    /// no comparison to make and nothing to report.
    /// </summary>
    [Test]
    public void Does_not_flag_a_root_with_no_final_segment()
    {
        Assert.That(RepoContextIndexedRootReporter.IsIdRootMismatch("lattice", "/"), Is.False);
    }
}
