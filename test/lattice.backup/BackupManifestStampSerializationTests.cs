using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Wire-compatibility coverage for the capturing-cluster stamp on
/// <see cref="BackupManifest"/>. The stamp is a strictly additive
/// <c>[Id(15)]</c> field, so a manifest that carries it round-trips faithfully and
/// a legacy manifest that omits it (a capture taken before the stamp existed)
/// still deserializes, decoding the stamp to <see langword="null"/> rather than
/// failing. This proves a backup taken before replication was enabled remains
/// resolvable once replication is turned on.
/// </summary>
[TestFixture]
public sealed class BackupManifestStampSerializationTests
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
    public void Manifest_with_a_capturing_cluster_stamp_round_trips()
    {
        var original = BackupManifestModelTests.Sample(capturingClusterId: "cluster-eu");

        var decoded = RoundTrip(original);

        Assert.That(decoded.CapturingClusterId, Is.EqualTo("cluster-eu"));
    }

    [Test]
    public void Legacy_manifest_without_the_stamp_still_deserializes_to_null()
    {
        // A pre-stamp manifest carries no capturing cluster id; the additive field
        // must decode to null rather than throwing.
        var legacy = BackupManifestModelTests.Sample(capturingClusterId: null);

        var decoded = RoundTrip(legacy);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.CapturingClusterId, Is.Null);
            Assert.That(decoded.Id, Is.EqualTo(legacy.Id));
            Assert.That(decoded.Kind, Is.EqualTo(legacy.Kind));
        });
    }
}
