using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Round-trips the dead-letter read-model DTOs through Orleans serialization to
/// prove the new wire surface is coherent and stable across the transport
/// boundary.
/// </summary>
[TestFixture]
public sealed class DeadLetterDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void DeadLetterEntryRecord_round_trips()
    {
        var original = new DeadLetterEntryRecord
        {
            Key = "user/42",
            ValuePreview = [1, 2, 3, 4],
            ValueByteLength = 4096,
            Reason = "value failed required-field validation",
            Source = DeadLetterSourceKind.Replication,
            TimestampUtc = DateTimeOffset.UnixEpoch.AddSeconds(1234),
            PreviewTruncated = true,
        };

        var restored = RoundTrip(original);

        // A byte[] property makes record value-equality reference-based, so
        // compare field-by-field (the preview bytes explicitly).
        Assert.Multiple(() =>
        {
            Assert.That(restored.Key, Is.EqualTo(original.Key));
            Assert.That(restored.ValuePreview, Is.EqualTo(original.ValuePreview));
            Assert.That(restored.ValueByteLength, Is.EqualTo(original.ValueByteLength));
            Assert.That(restored.Reason, Is.EqualTo(original.Reason));
            Assert.That(restored.Source, Is.EqualTo(original.Source));
            Assert.That(restored.TimestampUtc, Is.EqualTo(original.TimestampUtc));
            Assert.That(restored.PreviewTruncated, Is.EqualTo(original.PreviewTruncated));
        });
    }

    [Test]
    public void DeadLetterQueueRequest_round_trips()
    {
        var original = new DeadLetterQueueRequest
        {
            TreeId = "tree-a",
            PageSize = 250,
            PageToken = "100",
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void DeadLetterQueuePage_round_trips()
    {
        var original = new DeadLetterQueuePage
        {
            Entries =
            [
                new DeadLetterEntryRecord
                {
                    Key = "k1",
                    ValuePreview = [9, 9],
                    ValueByteLength = 2,
                    Reason = "r1",
                    Source = DeadLetterSourceKind.Restore,
                    TimestampUtc = DateTimeOffset.UnixEpoch,
                },
            ],
            NextPageToken = "1",
        };

        var restored = RoundTrip(original);
        Assert.That(restored.NextPageToken, Is.EqualTo("1"));
        Assert.That(restored.Entries, Has.Count.EqualTo(1));
        Assert.That(restored.Entries[0].Key, Is.EqualTo("k1"));
        Assert.That(restored.Entries[0].Source, Is.EqualTo(DeadLetterSourceKind.Restore));
    }

    [Test]
    public void DeadLetterSourceKind_values_are_stable()
    {
        // The wire mapping depends on these ordinals; a reorder would silently
        // remap a persisted / in-flight source, so pin them.
        Assert.Multiple(() =>
        {
            Assert.That((int)DeadLetterSourceKind.Replication, Is.EqualTo(0));
            Assert.That((int)DeadLetterSourceKind.Restore, Is.EqualTo(1));
            Assert.That((int)DeadLetterSourceKind.LocalRejected, Is.EqualTo(2));
            Assert.That((int)DeadLetterSourceKind.Unknown, Is.EqualTo(3));
        });
    }
}
