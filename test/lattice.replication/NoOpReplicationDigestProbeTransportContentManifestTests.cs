using NUnit.Framework;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the content-hash manifest-exchange seam on
/// <see cref="IReplicationDigestProbeTransport"/>: the default interface
/// method and the <c>NoOpReplicationDigestProbeTransport</c> implementation
/// both report the exchange as unsupported, which is the rolling-upgrade-safe
/// fallback that keeps the wire byte-identical to today.
/// </summary>
[TestFixture]
public sealed class NoOpReplicationDigestProbeTransportContentManifestTests
{
    // A minimal transport that does NOT override ExchangeContentManifestAsync,
    // exercising the interface default method.
    private sealed class DefaultMethodTransport : IReplicationDigestProbeTransport
    {
        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new DigestProbeResponse { DigestAvailable = false });
    }

    [Test]
    public async Task Default_interface_method_reports_exchange_unsupported()
    {
        IReplicationDigestProbeTransport transport = new DefaultMethodTransport();
        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = "site-a" };

        var response = await transport.ExchangeContentManifestAsync("site-b", request, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.False);
            Assert.That(response.MissingEntryIndices, Is.Empty);
        });
    }

    [Test]
    public void Noop_transport_can_be_constructed_and_implements_seam()
    {
        // NoOpReplicationDigestProbeTransport is internal; the assembly grants
        // InternalsVisibleTo to the test project, so it is constructible here.
        var transport = new NoOpReplicationDigestProbeTransport();

        Assert.That(transport, Is.InstanceOf<IReplicationDigestProbeTransport>());
    }

    [Test]
    public async Task Noop_transport_reports_exchange_unsupported()
    {
        IReplicationDigestProbeTransport transport = new NoOpReplicationDigestProbeTransport();
        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = "site-a" };

        var response = await transport.ExchangeContentManifestAsync("site-b", request, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.False);
            Assert.That(response.MissingEntryIndices, Is.Empty);
        });
    }

    [TestCase("")]
    [TestCase(null)]
    public void Noop_transport_rejects_empty_target_cluster_id(string? targetClusterId)
    {
        var transport = new NoOpReplicationDigestProbeTransport();
        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = "site-a" };

        Assert.ThrowsAsync<ArgumentException>(() =>
            transport.ExchangeContentManifestAsync(targetClusterId!, request, CancellationToken.None));
    }

    [TestCase("")]
    [TestCase(null)]
    public void Noop_transport_rejects_empty_tree_name(string? treeName)
    {
        var transport = new NoOpReplicationDigestProbeTransport();
        var request = new ContentManifestRequest { TreeName = treeName!, OriginClusterId = "site-a" };

        Assert.ThrowsAsync<ArgumentException>(() =>
            transport.ExchangeContentManifestAsync("site-b", request, CancellationToken.None));
    }
}
