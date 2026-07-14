using System.Diagnostics.Metrics;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
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
    public void ApplyCallCredentials_on_plaintext_endpoint_warns_and_meters_and_uses_insecure_credentials()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var logger = new CapturingLogger();
        var channelOptions = new GrpcChannelOptions();
        var measurements = new List<KeyValuePair<string, object?>[]>();
        using var listener = ListenForInsecureChannel(measurements);

        GrpcChannelHardening.ApplyCallCredentials(
            channelOptions,
            new Uri("http://peer.example/"),
            allowPlaintextEndpoints: true,
            secrets,
            "peer-a",
            "self",
            logger,
            "push");

        Assert.Multiple(() =>
        {
            Assert.That(channelOptions.UnsafeUseInsecureChannelCallCredentials, Is.True);
            Assert.That(channelOptions.Credentials, Is.Not.Null);
            Assert.That(logger.Warnings, Has.Some.Contains("INSECURE plaintext channel"));
            Assert.That(logger.Warnings, Has.Some.Contains("peer-a"));
            Assert.That(measurements, Has.Count.EqualTo(1));
        });

        var tags = measurements[0];
        Assert.Multiple(() =>
        {
            Assert.That(tags, Has.Some.Matches<KeyValuePair<string, object?>>(
                t => t.Key == LatticeReplicationGrpcMetrics.TagPeer && Equals(t.Value, "peer-a")));
            Assert.That(tags, Has.Some.Matches<KeyValuePair<string, object?>>(
                t => t.Key == LatticeReplicationGrpcMetrics.TagTransport && Equals(t.Value, "push")));
        });
    }

    [Test]
    public void ApplyCallCredentials_on_https_endpoint_is_silent_and_uses_secure_credentials()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var logger = new CapturingLogger();
        var channelOptions = new GrpcChannelOptions();
        var measurements = new List<KeyValuePair<string, object?>[]>();
        using var listener = ListenForInsecureChannel(measurements);

        GrpcChannelHardening.ApplyCallCredentials(
            channelOptions,
            new Uri("https://peer.example/"),
            allowPlaintextEndpoints: true,
            secrets,
            "peer-a",
            "self",
            logger,
            "push");

        Assert.Multiple(() =>
        {
            Assert.That(channelOptions.UnsafeUseInsecureChannelCallCredentials, Is.False);
            Assert.That(channelOptions.Credentials, Is.Not.Null);
            Assert.That(logger.Warnings, Is.Empty);
            Assert.That(measurements, Is.Empty);
        });
    }

    [Test]
    public void ApplyCallCredentials_without_optin_does_not_take_the_insecure_path()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var logger = new CapturingLogger();
        var channelOptions = new GrpcChannelOptions();
        var measurements = new List<KeyValuePair<string, object?>[]>();
        using var listener = ListenForInsecureChannel(measurements);

        GrpcChannelHardening.ApplyCallCredentials(
            channelOptions,
            new Uri("http://peer.example/"),
            allowPlaintextEndpoints: false,
            secrets,
            "peer-a",
            "self",
            logger,
            "snapshot");

        Assert.Multiple(() =>
        {
            Assert.That(channelOptions.UnsafeUseInsecureChannelCallCredentials, Is.False);
            Assert.That(logger.Warnings, Is.Empty);
            Assert.That(measurements, Is.Empty);
        });
    }

    [Test]
    public void ApplyCallCredentials_throws_on_null_arguments()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var logger = new CapturingLogger();
        Assert.Multiple(() =>
        {
            Assert.That(
                () => GrpcChannelHardening.ApplyCallCredentials(
                    null!, new Uri("https://p/"), true, secrets, "p", "s", logger, "push"),
                Throws.ArgumentNullException);
            Assert.That(
                () => GrpcChannelHardening.ApplyCallCredentials(
                    new GrpcChannelOptions(), null!, true, secrets, "p", "s", logger, "push"),
                Throws.ArgumentNullException);
            Assert.That(
                () => GrpcChannelHardening.ApplyCallCredentials(
                    new GrpcChannelOptions(), new Uri("https://p/"), true, secrets, "p", "s", null!, "push"),
                Throws.ArgumentNullException);
        });
    }

    private static MeterListener ListenForInsecureChannel(List<KeyValuePair<string, object?>[]> sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == LatticeReplicationGrpcMetrics.MeterName
                    && instrument.Name == LatticeReplicationGrpcMetrics.InsecureChannelName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) => sink.Add(tags.ToArray()));
        listener.Start();
        return listener;
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
