using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Tests.DeadLetter;

/// <summary>
/// Unit tests for the explorer's dead-letter view models: the
/// <see cref="DeadLetterEntry.From"/> projection and the
/// <see cref="DeadLetterPage"/> convenience surface.
/// </summary>
[TestFixture]
public class DeadLetterEntryTests
{
    [Test]
    public void From_maps_every_field()
    {
        var record = new DeadLetterEntryRecord
        {
            Key = "k1",
            ValuePreview = new byte[] { 7, 8 },
            ValueByteLength = 4,
            PreviewTruncated = true,
            Reason = "bad",
            Source = DeadLetterSourceKind.Restore,
            TimestampUtc = new DateTimeOffset(2026, 5, 6, 7, 8, 9, TimeSpan.Zero),
        };

        var entry = DeadLetterEntry.From(record);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.EqualTo("k1"));
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 7, 8 }));
            Assert.That(entry.ValueByteLength, Is.EqualTo(4));
            Assert.That(entry.Truncated, Is.True);
            Assert.That(entry.Reason, Is.EqualTo("bad"));
            Assert.That(entry.Source, Is.EqualTo(DeadLetterSource.Restore));
            Assert.That(entry.TimestampUtc, Is.EqualTo(new DateTimeOffset(2026, 5, 6, 7, 8, 9, TimeSpan.Zero)));
        });
    }

    [Test]
    public void From_maps_an_unrecognised_source_to_unknown()
    {
        var record = new DeadLetterEntryRecord
        {
            Key = "k1",
            Reason = "r",
            Source = (DeadLetterSourceKind)999,
        };

        var entry = DeadLetterEntry.From(record);

        Assert.That(entry.Source, Is.EqualTo(DeadLetterSource.Unknown));
    }

    [Test]
    public void From_rejects_a_null_record()
    {
        Assert.That(() => DeadLetterEntry.From(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Page_Empty_has_no_entries_and_no_continuation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DeadLetterPage.Empty.Entries, Is.Empty);
            Assert.That(DeadLetterPage.Empty.ContinuationToken, Is.Null);
            Assert.That(DeadLetterPage.Empty.HasMore, Is.False);
        });
    }

    [Test]
    public void Page_HasMore_is_true_when_a_continuation_token_is_present()
    {
        var page = new DeadLetterPage { ContinuationToken = "next" };

        Assert.That(page.HasMore, Is.True);
    }
}
