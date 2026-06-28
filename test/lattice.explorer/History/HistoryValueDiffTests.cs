using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryValueDiffTests
{
    [Test]
    public void Compute_NullPrevious_ReturnsEmpty()
    {
        Assert.That(HistoryValueDiff.Compute(null, "anything"), Is.Empty);
    }

    [Test]
    public void Compute_IdenticalText_AllUnchanged()
    {
        var diff = HistoryValueDiff.Compute("a\nb\nc", "a\nb\nc");

        Assert.That(diff.Select(d => d.Kind), Is.All.EqualTo(HistoryDiffLineKind.Unchanged));
        Assert.That(diff, Has.Count.EqualTo(3));
    }

    [Test]
    public void Compute_AddedLine_MarksAdded()
    {
        var diff = HistoryValueDiff.Compute("a\nb", "a\nb\nc");

        Assert.That(diff.Where(d => d.Kind == HistoryDiffLineKind.Added).Select(d => d.Text),
            Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public void Compute_RemovedLine_MarksRemoved()
    {
        var diff = HistoryValueDiff.Compute("a\nb\nc", "a\nc");

        Assert.That(diff.Where(d => d.Kind == HistoryDiffLineKind.Removed).Select(d => d.Text),
            Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public void Compute_ReplacedLine_MarksRemovedThenAdded()
    {
        var diff = HistoryValueDiff.Compute("hello", "world");

        Assert.Multiple(() =>
        {
            Assert.That(diff.Any(d => d.Kind == HistoryDiffLineKind.Removed && d.Text == "hello"), Is.True);
            Assert.That(diff.Any(d => d.Kind == HistoryDiffLineKind.Added && d.Text == "world"), Is.True);
        });
    }

    [Test]
    public void Compute_NormalizesCrlf()
    {
        var diff = HistoryValueDiff.Compute("a\r\nb", "a\nb");

        Assert.That(diff.Select(d => d.Kind), Is.All.EqualTo(HistoryDiffLineKind.Unchanged));
    }
}
