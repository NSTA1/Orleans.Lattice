using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="CompressionDictionaryConvergence.ConvergeAsync"/>:
/// the receiver half of the self-distributing shared dictionary that pulls a
/// peer-advertised dictionary it does not yet hold, verifies the bytes against
/// the advertised fingerprint, installs them through the provider's sink, and
/// emits the convergence metric.
/// </summary>
[TestFixture]
public class CompressionDictionaryConvergenceTests
{
    private sealed class StubPullTransport : IReplicationDigestProbeTransport
    {
        private readonly Func<CompressionDictionaryPullRequest, CompressionDictionaryPullResponse> _responder;

        public StubPullTransport(
            Func<CompressionDictionaryPullRequest, CompressionDictionaryPullResponse> responder)
            => _responder = responder;

        public int PullCount { get; private set; }

        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)
            => Task.FromResult(default(DigestProbeResponse));

        public Task<CompressionDictionaryPullResponse> PullCompressionDictionaryAsync(
            string targetClusterId,
            CompressionDictionaryPullRequest request,
            CancellationToken cancellationToken)
        {
            PullCount++;
            return Task.FromResult(_responder(request));
        }
    }

    private static AutoTrainingCompressionDictionaryProvider NewProvider()
        => new(new CompressionDictionaryTrainingOptions { Enabled = true });

    private static CompressionDictionaryPullResponse Served(uint id, byte[] bytes)
        => new()
        {
            ExchangeSupported = true,
            Found = true,
            DictionaryId = id,
            Fingerprint = CompressionDictionaryFingerprint.Compute(bytes),
            Dictionary = bytes,
        };

    [Test]
    public async Task ConvergeAsync_installs_a_verified_pulled_dictionary()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var fp = CompressionDictionaryFingerprint.Compute(bytes);
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(7u, bytes));

        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", new[] { new AdvertisedCompressionDictionary(7u, fp) }, "tree", default);

        Assert.Multiple(() =>
        {
            Assert.That(installed, Is.EqualTo(1));
            Assert.That(provider.TryGetDictionary(7u, out var stored), Is.True);
            Assert.That(stored.ToArray(), Is.EqualTo(bytes));
        });
    }

    [Test]
    public async Task ConvergeAsync_rejects_bytes_that_do_not_match_the_advertised_fingerprint()
    {
        var bytes = new byte[] { 1, 2, 3 };
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(7u, bytes));

        // Advertise a fingerprint that does not match the served bytes.
        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", new[] { new AdvertisedCompressionDictionary(7u, 0xDEADUL) }, "tree", default);

        Assert.Multiple(() =>
        {
            Assert.That(installed, Is.EqualTo(0));
            Assert.That(provider.TryGetDictionary(7u, out _), Is.False);
        });
    }

    [Test]
    public async Task ConvergeAsync_skips_ids_already_held_locally()
    {
        var bytes = new byte[] { 8, 8, 8 };
        var fp = CompressionDictionaryFingerprint.Compute(bytes);
        using var provider = NewProvider();
        provider.TryInstall(7u, bytes);
        var transport = new StubPullTransport(_ => Served(7u, bytes));

        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", new[] { new AdvertisedCompressionDictionary(7u, fp) }, "tree", default);

        Assert.Multiple(() =>
        {
            Assert.That(installed, Is.EqualTo(0));
            Assert.That(transport.PullCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ConvergeAsync_skips_reserved_id_and_zero_fingerprint()
    {
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(1u, new byte[] { 1 }));

        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport,
            provider,
            "peer",
            new[]
            {
                new AdvertisedCompressionDictionary(0u, 123UL),
                new AdvertisedCompressionDictionary(2u, 0UL),
            },
            "tree",
            default);

        Assert.Multiple(() =>
        {
            Assert.That(installed, Is.EqualTo(0));
            Assert.That(transport.PullCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ConvergeAsync_is_idempotent_across_repeated_calls()
    {
        var bytes = new byte[] { 5, 6, 7 };
        var fp = CompressionDictionaryFingerprint.Compute(bytes);
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(4u, bytes));
        var advert = new[] { new AdvertisedCompressionDictionary(4u, fp) };

        var first = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", advert, "tree", default);
        var second = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", advert, "tree", default);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1));
            Assert.That(second, Is.EqualTo(0));
            Assert.That(transport.PullCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ConvergeAsync_is_a_no_op_when_the_provider_is_not_a_sink()
    {
        var transport = new StubPullTransport(_ => Served(7u, new byte[] { 1 }));

        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport,
            OperatorSuppliedCompressionDictionaryProvider.Empty,
            "peer",
            new[] { new AdvertisedCompressionDictionary(7u, 1UL) },
            "tree",
            default);

        Assert.Multiple(() =>
        {
            Assert.That(installed, Is.EqualTo(0));
            Assert.That(transport.PullCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ConvergeAsync_is_a_no_op_for_empty_or_null_advertisement()
    {
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(7u, new byte[] { 1 }));

        var nullCount = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", null, "tree", default);
        var emptyCount = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", Array.Empty<AdvertisedCompressionDictionary>(), "tree", default);

        Assert.Multiple(() =>
        {
            Assert.That(nullCount, Is.EqualTo(0));
            Assert.That(emptyCount, Is.EqualTo(0));
            Assert.That(transport.PullCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ConvergeAsync_emits_unavailable_metric_when_the_peer_does_not_hold_the_id()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DictionaryConvergenceName);
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => CompressionDictionaryPullResponse.NotHeld);

        var installed = await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", new[] { new AdvertisedCompressionDictionary(7u, 99UL) }, "tree", default);

        Assert.That(installed, Is.EqualTo(0));
        var measurement = collector.Measurements.Single();
        Assert.That(
            measurement.Tags.Single(t => t.Key == LatticeReplicationMetrics.TagOutcome).Value,
            Is.EqualTo(LatticeReplicationMetrics.DictionaryConvergenceOutcomeUnavailable));
    }

    [Test]
    public async Task ConvergeAsync_emits_installed_metric_on_success()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DictionaryConvergenceName);
        var bytes = new byte[] { 3, 1, 4, 1, 5 };
        var fp = CompressionDictionaryFingerprint.Compute(bytes);
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => Served(7u, bytes));

        await CompressionDictionaryConvergence.ConvergeAsync(
            transport, provider, "peer", new[] { new AdvertisedCompressionDictionary(7u, fp) }, "tree", default);

        var measurement = collector.Measurements.Single();
        Assert.That(
            measurement.Tags.Single(t => t.Key == LatticeReplicationMetrics.TagOutcome).Value,
            Is.EqualTo(LatticeReplicationMetrics.DictionaryConvergenceOutcomeInstalled));
    }

    [Test]
    public void ConvergeAsync_throws_on_null_transport_or_provider()
    {
        using var provider = NewProvider();
        var transport = new StubPullTransport(_ => CompressionDictionaryPullResponse.NotHeld);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => CompressionDictionaryConvergence.ConvergeAsync(null!, provider, "peer", null, "tree", default),
                Throws.ArgumentNullException);
            Assert.That(
                () => CompressionDictionaryConvergence.ConvergeAsync(transport, null!, "peer", null, "tree", default),
                Throws.ArgumentNullException);
        });
    }
}
