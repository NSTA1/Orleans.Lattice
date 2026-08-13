using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Round-trips the transport-agnostic data-API facade DTOs through the Orleans
/// serializer to prove the contract is coherent and stable. Every serializable
/// facade request / response record is covered so a field renumbering or alias
/// drift is caught here rather than at the wire.
/// </summary>
[TestFixture]
public sealed class DataDtoSerializationTests
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
    public void DataEntry_round_trips()
    {
        var original = new DataEntry { Key = "k1", Value = new byte[] { 1, 2, 3 } };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Key, Is.EqualTo("k1"));
            Assert.That(copy.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void DataAtomicBatch_round_trips_with_upserts_and_deletes()
    {
        var original = new DataAtomicBatch
        {
            Upserts = [new DataEntry { Key = "a", Value = new byte[] { 9 } }],
            DeleteKeys = ["b", "c"],
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Upserts.Select(e => e.Key), Is.EqualTo(new[] { "a" }));
            Assert.That(copy.DeleteKeys, Is.EqualTo(new[] { "b", "c" }));
        });
    }

    [Test]
    public void DataTreeBatch_round_trips()
    {
        var original = new DataTreeBatch
        {
            TreeId = "tree-a",
            Upserts = [new DataEntry { Key = "a", Value = new byte[] { 1 } }],
            DeleteKeys = ["z"],
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Upserts, Has.Count.EqualTo(1));
            Assert.That(copy.DeleteKeys, Is.EqualTo(new[] { "z" }));
        });
    }

    [Test]
    public void DataRangeRequest_round_trips_with_bounds_and_token()
    {
        var original = new DataRangeRequest
        {
            TreeId = "tree-a",
            StartInclusive = "a",
            EndExclusive = "m",
            PageSize = 50,
            ContinuationToken = "cursor-x",
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void DataRangePage_round_trips()
    {
        var original = new DataRangePage
        {
            TreeId = "tree-a",
            Entries = [new DataEntry { Key = "k0", Value = new byte[] { 7 } }],
            ContinuationToken = null,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k0" }));
            Assert.That(copy.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void DataReadResult_round_trips_found_and_absent()
    {
        var found = new DataReadResult { TreeId = "t", Key = "k", Found = true, Value = new byte[] { 5 } };
        var absent = new DataReadResult { TreeId = "t", Key = "missing", Found = false, Value = Array.Empty<byte>() };

        var foundCopy = RoundTrip(found);
        var absentCopy = RoundTrip(absent);
        Assert.Multiple(() =>
        {
            Assert.That(foundCopy.Found, Is.True);
            Assert.That(foundCopy.Value, Is.EqualTo(new byte[] { 5 }));
            Assert.That(absentCopy.Found, Is.False);
            Assert.That(absentCopy.Value, Is.Empty);
        });
    }

    [Test]
    public void DataRangeDeleteRequest_round_trips()
    {
        var original = new DataRangeDeleteRequest
        {
            TreeId = "tree-a",
            StartInclusive = "a",
            EndExclusive = "m",
        };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void DataRangeDeleteResult_round_trips()
    {
        var original = new DataRangeDeleteResult { TreeId = "tree-a", DeletedCount = 42 };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }
}
