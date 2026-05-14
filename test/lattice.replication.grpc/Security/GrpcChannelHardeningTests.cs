using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

[TestFixture]
public class GrpcChannelHardeningTests
{
    [Test]
    public void EnforceSchemeGate_allows_https()
    {
        Assert.DoesNotThrow(
            () => GrpcChannelHardening.EnforceSchemeGate(new Uri("https://peer.example/"), allowPlaintext: false, "peer"));
    }

    [Test]
    public void EnforceSchemeGate_rejects_http_when_plaintext_not_allowed()
    {
        Assert.That(
            () => GrpcChannelHardening.EnforceSchemeGate(new Uri("http://peer.example/"), allowPlaintext: false, "peer"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void EnforceSchemeGate_allows_http_when_plaintext_explicitly_allowed()
    {
        Assert.DoesNotThrow(
            () => GrpcChannelHardening.EnforceSchemeGate(new Uri("http://peer.example/"), allowPlaintext: true, "peer"));
    }

    [Test]
    public void EnforceSchemeGate_throws_on_null_endpoint()
    {
        Assert.That(
            () => GrpcChannelHardening.EnforceSchemeGate(null!, allowPlaintext: false, "peer"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EnforceSchemeGate_throws_on_null_peer()
    {
        Assert.That(
            () => GrpcChannelHardening.EnforceSchemeGate(new Uri("https://peer/"), allowPlaintext: false, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BuildCallCredentials_throws_on_null_arguments()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        Assert.That(() => GrpcChannelHardening.BuildCallCredentials(null!, "peer", "self"), Throws.ArgumentNullException);
        Assert.That(() => GrpcChannelHardening.BuildCallCredentials(s, null!, "self"), Throws.ArgumentNullException);
        Assert.That(() => GrpcChannelHardening.BuildCallCredentials(s, "peer", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void BuildCallCredentials_returns_non_null_credentials()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        var creds = GrpcChannelHardening.BuildCallCredentials(s, "peer", "self");
        Assert.That(creds, Is.Not.Null);
    }

    [Test]
    public async Task PopulateMetadataAsync_adds_secret_header_when_provider_returns_token()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync("peer-a", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("token-xyz"));
        var md = new global::Grpc.Core.Metadata();

        await GrpcChannelHardening.PopulateMetadataAsync(s, "peer-a", "self", md, CancellationToken.None);

        Assert.That(md.GetValue(LatticeReplicationGrpcMetadataNames.SecretHeader), Is.EqualTo("token-xyz"));
    }

    [Test]
    public async Task PopulateMetadataAsync_always_adds_origin_header()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("token"));
        var md = new global::Grpc.Core.Metadata();

        await GrpcChannelHardening.PopulateMetadataAsync(s, "peer-a", "site-a", md, CancellationToken.None);

        Assert.That(md.GetValue(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader), Is.EqualTo("site-a"));
    }

    [Test]
    public async Task PopulateMetadataAsync_omits_secret_header_when_provider_returns_null()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        var md = new global::Grpc.Core.Metadata();

        await GrpcChannelHardening.PopulateMetadataAsync(s, "peer-a", "site-a", md, CancellationToken.None);

        Assert.That(md.GetValue(LatticeReplicationGrpcMetadataNames.SecretHeader), Is.Null);
        Assert.That(md.GetValue(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader), Is.EqualTo("site-a"));
    }

    [Test]
    public async Task PopulateMetadataAsync_omits_secret_header_when_provider_returns_empty_string()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>(string.Empty));
        var md = new global::Grpc.Core.Metadata();

        await GrpcChannelHardening.PopulateMetadataAsync(s, "peer-a", "site-a", md, CancellationToken.None);

        Assert.That(md.GetValue(LatticeReplicationGrpcMetadataNames.SecretHeader), Is.Null);
    }
}
