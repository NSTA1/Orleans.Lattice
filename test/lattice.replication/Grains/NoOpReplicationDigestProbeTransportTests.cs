using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the default no-op digest-probe transport.
/// </summary>
[TestFixture]
public class NoOpReplicationDigestProbeTransportTests
{
    private static readonly IReplicationDigestProbeTransport Transport =
        new NoOpReplicationDigestProbeTransport();

    [Test]
    public async Task ProbeDigestAsync_reports_remote_unavailable()
    {
        var response = await Transport.ProbeDigestAsync(
            "site-b",
            new DigestProbeRequest { TreeName = "tree", ShardIndex = 0 },
            CancellationToken.None);

        Assert.That(response.DigestAvailable, Is.False);
    }

    [TestCase("")]
    [TestCase(null)]
    public void ProbeDigestAsync_throws_when_target_cluster_is_empty(string? target)
    {
        Assert.That(
            async () => await Transport.ProbeDigestAsync(
                target!,
                new DigestProbeRequest { TreeName = "tree", ShardIndex = 0 },
                CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void ProbeDigestAsync_throws_when_tree_name_is_empty()
    {
        Assert.That(
            async () => await Transport.ProbeDigestAsync(
                "site-b",
                new DigestProbeRequest { TreeName = "", ShardIndex = 0 },
                CancellationToken.None),
            Throws.ArgumentException);
    }
}
