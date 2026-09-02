using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Direct coverage for <see cref="LeafSnapshotRowSequence"/>, the
/// encoding-agnostic row view every snapshot consumer reads through. Its whole
/// purpose is that a caller cannot tell which encoding a blob used, so the
/// tests assert the two backings behave identically - including the empty
/// case, repeat enumeration, and the yielded row count.
/// </summary>
[TestFixture]
public sealed class LeafSnapshotRowSequenceTests
{
    private static LeafSnapshotRow[] SampleRows() =>
    [
        new("a", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("1"), new HybridLogicalClock { WallClockTicks = 1L })),
        new("b", LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("2"), new HybridLogicalClock { WallClockTicks = 2L })),
    ];

    private static List<string> Keys(LeafSnapshotRowSequence sequence)
    {
        var keys = new List<string>();
        foreach (var row in sequence)
        {
            keys.Add(row.Key);
        }

        return keys;
    }

    [Test]
    public void Empty_yields_nothing_and_reports_a_zero_count()
    {
        var sequence = LeafSnapshotRowSequence.Empty;

        Assert.That(sequence.Count, Is.Zero);
        Assert.That(Keys(sequence), Is.Empty);
    }

    [Test]
    public void FromLegacyRows_yields_the_list_in_order_and_treats_null_or_empty_as_empty()
    {
        var sequence = LeafSnapshotRowSequence.FromLegacyRows(SampleRows());

        Assert.That(sequence.Count, Is.EqualTo(2));
        Assert.That(Keys(sequence), Is.EqualTo(new[] { "a", "b" }).AsCollection);
        Assert.That(LeafSnapshotRowSequence.FromLegacyRows(null).Count, Is.Zero);
        Assert.That(LeafSnapshotRowSequence.FromLegacyRows(Array.Empty<LeafSnapshotRow>()).Count, Is.Zero);
    }

    [Test]
    public void FromFrame_yields_the_same_rows_as_the_legacy_backing()
    {
        var rows = SampleRows();
        var fromFrame = LeafSnapshotRowSequence.FromFrame(LeafSnapshotCodec.Encode(rows));

        Assert.That(fromFrame.Count, Is.EqualTo(2));
        Assert.That(Keys(fromFrame), Is.EqualTo(Keys(LeafSnapshotRowSequence.FromLegacyRows(rows))).AsCollection);
    }

    [Test]
    public void FromFrame_reports_an_empty_sequence_for_a_buffer_whose_header_is_unreadable()
    {
        // Consistent with GetRowCount: an unreadable header means "no rows",
        // and the fail-closed rejection of that blob is ValidateRowPayload's
        // job, not the sequence's.
        var sequence = LeafSnapshotRowSequence.FromFrame(Encoding.UTF8.GetBytes("{\"Rows\":[]}"));

        Assert.That(sequence.Count, Is.Zero);
        Assert.That(Keys(sequence), Is.Empty);
    }

    [Test]
    public void FromFrame_rejects_a_null_frame()
        => Assert.That(() => LeafSnapshotRowSequence.FromFrame(null!), Throws.ArgumentNullException);

    [Test]
    public void A_sequence_can_be_enumerated_more_than_once_with_the_same_result()
    {
        // The enumerator carries the walk position, not the sequence, so a
        // second foreach must start over rather than continue where the first
        // stopped.
        var sequence = LeafSnapshotRowSequence.FromFrame(LeafSnapshotCodec.Encode(SampleRows()));

        Assert.That(Keys(sequence), Is.EqualTo(new[] { "a", "b" }).AsCollection);
        Assert.That(Keys(sequence), Is.EqualTo(new[] { "a", "b" }).AsCollection);
    }
}
