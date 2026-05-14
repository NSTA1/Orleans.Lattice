using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class CachingReplicationSecretProviderTests
{
    private static IOptionsMonitor<LatticeReplicationSecurityOptions> OptionsFor(TimeSpan refresh)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        m.CurrentValue.Returns(new LatticeReplicationSecurityOptions { SecretRefreshInterval = refresh });
        return m;
    }

    private sealed class CountingSource : ILatticeReplicationSecretSource
    {
        public int OutboundCalls;
        public int AcceptedCalls;
        public string? Secret;
        public LatticeReplicationAcceptedSecrets Snapshot = LatticeReplicationAcceptedSecrets.Empty;

        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref OutboundCalls);
            return new ValueTask<string?>(Secret);
        }

        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AcceptedCalls);
            return new ValueTask<LatticeReplicationAcceptedSecrets>(Snapshot);
        }
    }

    [Test]
    public void Constructor_throws_on_null_source()
    {
        Assert.That(
            () => new CachingReplicationSecretProvider(null!, OptionsFor(TimeSpan.FromSeconds(30)), TimeProvider.System),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(
            () => new CachingReplicationSecretProvider(new CountingSource(), null!, TimeProvider.System),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_time_provider()
    {
        Assert.That(
            () => new CachingReplicationSecretProvider(new CountingSource(), OptionsFor(TimeSpan.FromSeconds(30)), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetOutboundSecretAsync_caches_within_refresh_interval()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new CountingSource { Secret = "alpha" };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(60)), time);

        _ = await p.GetOutboundSecretAsync("peer", CancellationToken.None);
        _ = await p.GetOutboundSecretAsync("peer", CancellationToken.None);
        _ = await p.GetOutboundSecretAsync("peer", CancellationToken.None);

        Assert.That(source.OutboundCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task GetOutboundSecretAsync_refreshes_after_interval_elapses()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new CountingSource { Secret = "alpha" };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(10)), time);

        _ = await p.GetOutboundSecretAsync("peer", CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(20));
        _ = await p.GetOutboundSecretAsync("peer", CancellationToken.None);

        Assert.That(source.OutboundCalls, Is.EqualTo(2));
    }

    [Test]
    public void GetOutboundSecretAsync_throws_when_peer_id_null()
    {
        var p = new CachingReplicationSecretProvider(new CountingSource(), OptionsFor(TimeSpan.FromSeconds(30)), TimeProvider.System);
        Assert.That(
            async () => await p.GetOutboundSecretAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_caches_within_refresh_interval()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new CountingSource
        {
            Snapshot = new LatticeReplicationAcceptedSecrets(new[] { "alpha" }, "v1"),
        };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(60)), time);

        var s1 = await p.GetAcceptedSecretsAsync(CancellationToken.None);
        var s2 = await p.GetAcceptedSecretsAsync(CancellationToken.None);

        Assert.That(source.AcceptedCalls, Is.EqualTo(1));
        Assert.That(s2, Is.SameAs(s1));
    }

    [Test]
    public async Task IsAcceptedAsync_returns_false_for_null_or_empty()
    {
        var p = new CachingReplicationSecretProvider(new CountingSource(), OptionsFor(TimeSpan.FromSeconds(30)), TimeProvider.System);
        Assert.That(await p.IsAcceptedAsync(null, CancellationToken.None), Is.False);
        Assert.That(await p.IsAcceptedAsync(string.Empty, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task IsAcceptedAsync_returns_true_when_presented_matches_any_accepted_secret()
    {
        var source = new CountingSource
        {
            Snapshot = new LatticeReplicationAcceptedSecrets(new[] { "alpha", "beta" }, "v1"),
        };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(30)), TimeProvider.System);

        Assert.That(await p.IsAcceptedAsync("beta", CancellationToken.None), Is.True);
    }

    [Test]
    public async Task IsAcceptedAsync_returns_false_when_presented_does_not_match()
    {
        var source = new CountingSource
        {
            Snapshot = new LatticeReplicationAcceptedSecrets(new[] { "alpha" }, "v1"),
        };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(30)), TimeProvider.System);

        Assert.That(await p.IsAcceptedAsync("not-a-match", CancellationToken.None), Is.False);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_refreshes_after_interval_elapses()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new CountingSource
        {
            Snapshot = new LatticeReplicationAcceptedSecrets(new[] { "alpha" }, "v1"),
        };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(10)), time);

        _ = await p.GetAcceptedSecretsAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(20));
        _ = await p.GetAcceptedSecretsAsync(CancellationToken.None);

        Assert.That(source.AcceptedCalls, Is.EqualTo(2));
    }

    [Test]
    public async Task GetOutboundSecretAsync_keeps_per_peer_cache_entries_isolated()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var perPeer = new Dictionary<string, string?> { ["peer-a"] = "secret-a", ["peer-b"] = "secret-b" };
        var source = new PerPeerSource(perPeer);
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(60)), time);

        var a = await p.GetOutboundSecretAsync("peer-a", CancellationToken.None);
        var b = await p.GetOutboundSecretAsync("peer-b", CancellationToken.None);
        var a2 = await p.GetOutboundSecretAsync("peer-a", CancellationToken.None);

        Assert.That(a, Is.EqualTo("secret-a"));
        Assert.That(b, Is.EqualTo("secret-b"));
        Assert.That(a2, Is.EqualTo("secret-a"));
        Assert.That(source.CallCount("peer-a"), Is.EqualTo(1));
        Assert.That(source.CallCount("peer-b"), Is.EqualTo(1));
    }

    [Test]
    public void GetOutboundSecretAsync_surfaces_source_exception_rather_than_returning_stale_value()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new ThrowingSource();
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromMilliseconds(1)), time);

        Assert.That(
            async () => await p.GetOutboundSecretAsync("peer", CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public void GetAcceptedSecretsAsync_surfaces_source_exception_rather_than_returning_stale_value()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new ThrowingSource();
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromMilliseconds(1)), time);

        Assert.That(
            async () => await p.GetAcceptedSecretsAsync(CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_concurrent_callers_share_one_source_fetch()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var source = new BlockingSource
        {
            Snapshot = new LatticeReplicationAcceptedSecrets(new[] { "alpha" }, "v1"),
        };
        var p = new CachingReplicationSecretProvider(source, OptionsFor(TimeSpan.FromSeconds(60)), time);

        // Spin up several concurrent callers while the source is blocked.
        var tasks = new Task<LatticeReplicationAcceptedSecrets>[8];
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = p.GetAcceptedSecretsAsync(CancellationToken.None).AsTask();
        }

        // Release the source so the gate-holder can complete.
        source.Release();
        var results = await Task.WhenAll(tasks);

        Assert.That(source.Calls, Is.EqualTo(1));
        Assert.That(results, Has.All.SameAs(results[0]));
    }

    private sealed class PerPeerSource : ILatticeReplicationSecretSource
    {
        private readonly Dictionary<string, string?> _map;
        private readonly Dictionary<string, int> _calls = new(StringComparer.Ordinal);
        public PerPeerSource(Dictionary<string, string?> map) { _map = map; }
        public int CallCount(string peer) => _calls.TryGetValue(peer, out var v) ? v : 0;

        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
        {
            _calls[peerClusterId] = CallCount(peerClusterId) + 1;
            return new ValueTask<string?>(_map.TryGetValue(peerClusterId, out var v) ? v : null);
        }
        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
            => new(LatticeReplicationAcceptedSecrets.Empty);
    }

    private sealed class ThrowingSource : ILatticeReplicationSecretSource
    {
        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("upstream unavailable");
        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
            => throw new InvalidOperationException("upstream unavailable");
    }

    private sealed class BlockingSource : ILatticeReplicationSecretSource
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Calls;
        public LatticeReplicationAcceptedSecrets Snapshot = LatticeReplicationAcceptedSecrets.Empty;

        public void Release() => _gate.TrySetResult();

        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
            => new((string?)null);
        public async ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            await _gate.Task.ConfigureAwait(false);
            return Snapshot;
        }
    }

    /// <summary>
    /// Minimal stub TimeProvider so we don't take a Microsoft.Extensions.TimeProvider.Testing reference.
    /// </summary>
    private sealed class FakeTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;
        public FakeTimeProvider(DateTimeOffset start) { _now = start; }
        public void Advance(TimeSpan by) => _now += by;
        public override DateTimeOffset GetUtcNow() => _now;
    }
}
