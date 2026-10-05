using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Guards on the production gRPC auth-scheme probe. The probe's happy path opens
/// a real channel (covered by end-to-end tests); these focus on the argument
/// contract and disposability without any network dependency.
/// </summary>
[TestFixture]
public class GrpcExplorerAuthSchemeProbeTests
{
    [Test]
    public void ProbeAsync_nullAddress_throws()
    {
        using var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(async () => await probe.ProbeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeAsync_whitespaceAddress_throws()
    {
        using var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(async () => await probe.ProbeAsync("   "), Throws.ArgumentException);
    }

    [Test]
    public void Dispose_isIdempotent()
    {
        var probe = new GrpcExplorerAuthSchemeProbe();
        Assert.That(() =>
        {
            probe.Dispose();
            probe.Dispose();
        }, Throws.Nothing);
    }

    [Test]
    public void IsCallerCancellation_cancelledStatus_cancelledToken_isTrue()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            GrpcExplorerAuthSchemeProbe.IsCallerCancellation(new RpcException(new Status(StatusCode.Cancelled, "cancelled")), cancelled.Token),
            Is.True);
    }

    [Test]
    public void IsCallerCancellation_cancelledStatus_liveToken_isFalse()
    {
        // The channel torn down under the call is not the caller giving up, so
        // the probe keeps its documented fall-back to an empty advertisement.
        Assert.That(
            GrpcExplorerAuthSchemeProbe.IsCallerCancellation(new RpcException(new Status(StatusCode.Cancelled, "cancelled")), CancellationToken.None),
            Is.False);
    }

    [TestCase(StatusCode.Unavailable)]
    [TestCase(StatusCode.Unimplemented)]
    [TestCase(StatusCode.DeadlineExceeded)]
    [TestCase(StatusCode.PermissionDenied)]
    public void IsCallerCancellation_otherStatus_cancelledToken_isFalse(StatusCode status)
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            GrpcExplorerAuthSchemeProbe.IsCallerCancellation(new RpcException(new Status(status, "failed")), cancelled.Token),
            Is.False);
    }

    [Test]
    public void The_probes_channel_leaves_cancellation_surfacing_as_an_RpcException()
    {
        // Why the probe needs the RpcException(Cancelled) arm at all, and why its
        // OperationCanceledException arm cannot be reached through the transport:
        // Grpc.Net.Client raises OperationCanceledException only when a channel
        // opts in with ThrowOperationCanceledOnCancellation, and the Explorer's
        // factory never sets it. Flipping that option would make the cancellation
        // arms swap roles, so pin it here rather than leaving the comment on the
        // catch block as the only record.
        var options = LatticeGrpcChannelFactory.BuildChannelOptions(new LatticeConnectionSettings
        {
            Address = "http://localhost:5000",
            AllowUnencryptedHttp2 = true,
        });

        Assert.That(
            options.ThrowOperationCanceledOnCancellation,
            Is.False,
            "a cancelled probe call reaches the probe as RpcException(Cancelled), never as OperationCanceledException");
    }
}
