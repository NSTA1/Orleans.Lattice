using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Coverage for <see cref="ReplicationOriginBindingStartupCheck"/>: origin binding on
/// with only a cluster-wide secret is warned about, never refused.
/// </summary>
[TestFixture]
public class ReplicationOriginBindingStartupCheckTests
{
    private const string Shared = "cluster-wide-secret";

    [Test]
    public async Task A_cluster_wide_secret_for_every_origin_is_warned_about()
    {
        var logger = new CapturingLogger();
        var check = Create(SecretsFor(fallback: Shared), Topology("site-a", "site-b"), logger);

        await check.StartAsync(CancellationToken.None);

        Assert.That(logger.Warnings, Has.Count.EqualTo(1));
        Assert.That(logger.Warnings[0], Does.Contain("cluster-wide"));
    }

    [Test]
    public async Task A_per_peer_secret_is_not_warned_about()
    {
        var logger = new CapturingLogger();
        var check = Create(SecretsFor(fallback: Shared, ("site-a", "secret-a")), Topology("site-a"), logger);

        await check.StartAsync(CancellationToken.None);

        Assert.That(logger.Warnings, Is.Empty);
    }

    [Test]
    public async Task Per_peer_secrets_with_no_fallback_are_not_warned_about()
    {
        var check = Create(SecretsFor(fallback: null, ("site-a", "secret-a")), Topology("site-a"), new CapturingLogger());

        Assert.That(await check.IsClusterWideOnlyAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public async Task Binding_off_is_not_checked()
    {
        var check = Create(
            SecretsFor(fallback: Shared),
            Topology("site-a"),
            new CapturingLogger(),
            new LatticeReplicationSecurityOptions { BindCredentialToOriginCluster = false });

        Assert.That(await check.IsClusterWideOnlyAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public void A_secret_source_that_throws_never_fails_startup()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<string?>>(_ => throw new InvalidOperationException("unavailable"));
        var check = Create(secrets, Topology("site-a"), new CapturingLogger());

        Assert.That(async () => await check.StartAsync(CancellationToken.None), Throws.Nothing);
    }

    private static ReplicationOriginBindingStartupCheck Create(
        IReplicationSecretProvider secrets,
        IReplicationTopology topology,
        ILogger<ReplicationOriginBindingStartupCheck> logger,
        LatticeReplicationSecurityOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        monitor.CurrentValue.Returns(options ?? new LatticeReplicationSecurityOptions());
        return new ReplicationOriginBindingStartupCheck(secrets, monitor, logger, topology);
    }

    private static IReplicationSecretProvider SecretsFor(string? fallback, params (string Peer, string Secret)[] perPeer)
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var origin = ci.ArgAt<string>(0);
                foreach (var (peer, secret) in perPeer)
                {
                    if (peer == origin)
                    {
                        return new ValueTask<string?>(secret);
                    }
                }

                return new ValueTask<string?>(fallback);
            });
        return secrets;
    }

    private static IReplicationTopology Topology(params string[] peers)
    {
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);
        return topology;
    }

    private sealed class CapturingLogger : ILogger<ReplicationOriginBindingStartupCheck>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
