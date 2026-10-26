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

    [Test]
    public void Compute_null_current_rejects_input_even_without_a_previous_revision()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => HistoryValueDiff.Compute(null, null!));

        Assert.That(exception!.ParamName, Is.EqualTo("current"));
    }

    [Test]
    public void Compute_inserted_middle_line_preserves_the_following_common_suffix()
    {
        var diff = HistoryValueDiff.Compute("a\nc", "a\nb\nc");

        Assert.That(diff, Is.EqualTo(new[]
        {
            new HistoryDiffLine(HistoryDiffLineKind.Unchanged, "a"),
            new HistoryDiffLine(HistoryDiffLineKind.Added, "b"),
            new HistoryDiffLine(HistoryDiffLineKind.Unchanged, "c"),
        }));
    }

    [Test]
    public void Compute_equal_length_alternatives_remove_before_adding()
    {
        var diff = HistoryValueDiff.Compute("a\nb", "b\na");

        Assert.That(diff, Is.EqualTo(new[]
        {
            new HistoryDiffLine(HistoryDiffLineKind.Removed, "a"),
            new HistoryDiffLine(HistoryDiffLineKind.Unchanged, "b"),
            new HistoryDiffLine(HistoryDiffLineKind.Added, "a"),
        }));
    }

    [Test]
    public void Compute_removed_tail_preserves_empty_lines_and_ordinal_case()
    {
        var diff = HistoryValueDiff.Compute("a\n\nB\n", "a\n\nb");

        Assert.That(diff, Is.EqualTo(new[]
        {
            new HistoryDiffLine(HistoryDiffLineKind.Unchanged, "a"),
            new HistoryDiffLine(HistoryDiffLineKind.Unchanged, ""),
            new HistoryDiffLine(HistoryDiffLineKind.Removed, "B"),
            new HistoryDiffLine(HistoryDiffLineKind.Removed, ""),
            new HistoryDiffLine(HistoryDiffLineKind.Added, "b"),
        }));
    }

    [TestCase("", "")]
    [TestCase("a\nb\n", "")]
    [TestCase("", "a\nb\n")]
    [TestCase("a\nb\nc", "c\nb\na")]
    [TestCase("a\na\nb", "a\nb\nb")]
    [TestCase("a\nb\nc", "a")]
    public void Compute_edits_reconstruct_both_original_sequences(string previous, string current)
    {
        var diff = HistoryValueDiff.Compute(previous, current);

        Assert.Multiple(() =>
        {
            Assert.That(diff.Where(line => line.Kind != HistoryDiffLineKind.Added).Select(line => line.Text),
                Is.EqualTo(previous.Split('\n')));
            Assert.That(diff.Where(line => line.Kind != HistoryDiffLineKind.Removed).Select(line => line.Text),
                Is.EqualTo(current.Split('\n')));
        });
    }
}
