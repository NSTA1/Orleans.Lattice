namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextEmbeddingCoverage"/>: a source is "covered"
/// (not an embedding gap) when it has a real landed vector or a contentless
/// "considered, no passages" marker, so the always-on gap sweep and the
/// unchanged-file selection stop re-driving a file that was read and found empty.
/// </summary>
[TestFixture]
public sealed class RepoContextEmbeddingCoverageTests
{
    private static RepoContextEmbeddingCoverage Coverage(string[] embedded, string[] contentless)
        => new(
            new HashSet<string>(embedded, StringComparer.Ordinal),
            new HashSet<string>(contentless, StringComparer.Ordinal));

    [Test]
    public void IsCovered_is_true_for_an_embedded_source()
    {
        var coverage = Coverage(new[] { "aaaa" }, Array.Empty<string>());

        Assert.That(coverage.IsCovered("aaaa"), Is.True,
            "A source with a real landed vector is covered.");
    }

    [Test]
    public void IsCovered_is_true_for_a_contentless_marked_source()
    {
        var coverage = Coverage(Array.Empty<string>(), new[] { "bbbb" });

        Assert.That(coverage.IsCovered("bbbb"), Is.True,
            "A source considered and found contentless is covered, so it is not a gap.");
    }

    [Test]
    public void IsCovered_is_false_for_an_unknown_source()
    {
        var coverage = Coverage(new[] { "aaaa" }, new[] { "bbbb" });

        Assert.That(coverage.IsCovered("cccc"), Is.False,
            "A source that is neither embedded nor marked is an uncovered gap.");
    }

    [Test]
    public void The_two_sets_are_kept_distinct()
    {
        var coverage = Coverage(new[] { "aaaa" }, new[] { "bbbb" });

        Assert.Multiple(() =>
        {
            Assert.That(coverage.Embedded, Does.Contain("aaaa"));
            Assert.That(coverage.Embedded, Does.Not.Contain("bbbb"),
                "A contentless marker is not reported as an embedded source.");
            Assert.That(coverage.Contentless, Does.Contain("bbbb"));
            Assert.That(coverage.Contentless, Does.Not.Contain("aaaa"));
        });
    }
}
