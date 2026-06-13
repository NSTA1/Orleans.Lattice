using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for the self-distributing shared-dictionary pull message
/// pair (<see cref="CompressionDictionaryPullRequest"/> and
/// <see cref="CompressionDictionaryPullResponse"/>): value semantics, the
/// canned <see cref="CompressionDictionaryPullResponse.NotSupported"/> /
/// <see cref="CompressionDictionaryPullResponse.NotHeld"/> sentinels, and
/// Orleans wire round-trips.
/// </summary>
[TestFixture]
public class CompressionDictionaryPullMessageTests
{
    [Test]
    public void Request_carries_the_requested_id()
    {
        var request = new CompressionDictionaryPullRequest { DictionaryId = 17u };

        Assert.That(request.DictionaryId, Is.EqualTo(17u));
    }

    [Test]
    public void NotSupported_reports_no_exchange_and_no_bytes()
    {
        var response = CompressionDictionaryPullResponse.NotSupported;

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.False);
            Assert.That(response.Found, Is.False);
            Assert.That(response.Dictionary.IsEmpty, Is.True);
        });
    }

    [Test]
    public void NotHeld_reports_supported_exchange_without_bytes()
    {
        var response = CompressionDictionaryPullResponse.NotHeld;

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.True);
            Assert.That(response.Found, Is.False);
            Assert.That(response.Dictionary.IsEmpty, Is.True);
        });
    }

    [Test]
    public void Request_round_trips_via_orleans_serializer()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = sp.GetRequiredService<Serializer<CompressionDictionaryPullRequest>>();

        var request = new CompressionDictionaryPullRequest { DictionaryId = 99u };
        var decoded = serializer.Deserialize(serializer.SerializeToArray(request));

        Assert.That(decoded.DictionaryId, Is.EqualTo(99u));
    }

    [Test]
    public void Response_round_trips_all_fields_via_orleans_serializer()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = sp.GetRequiredService<Serializer<CompressionDictionaryPullResponse>>();

        var response = new CompressionDictionaryPullResponse
        {
            ExchangeSupported = true,
            Found = true,
            DictionaryId = 5u,
            Fingerprint = 0xCAFEF00DUL,
            Dictionary = new byte[] { 7, 8, 9, 10 },
        };

        var decoded = serializer.Deserialize(serializer.SerializeToArray(response));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ExchangeSupported, Is.True);
            Assert.That(decoded.Found, Is.True);
            Assert.That(decoded.DictionaryId, Is.EqualTo(5u));
            Assert.That(decoded.Fingerprint, Is.EqualTo(0xCAFEF00DUL));
            Assert.That(decoded.Dictionary.ToArray(), Is.EqualTo(new byte[] { 7, 8, 9, 10 }));
        });
    }
}
