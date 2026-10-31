using System.Net;
using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The transient/definite classification of a failed capability probe (issue
/// #5420), read from the gRPC status code wherever there is one, with a denial
/// anywhere in the chain always definite (issue #5419).
/// </summary>
[TestFixture]
public sealed class AreaProbeFaultsTests
{
    [TestCase(StatusCode.Unavailable, true)]
    [TestCase(StatusCode.Internal, true)]
    [TestCase(StatusCode.DeadlineExceeded, true)]
    [TestCase(StatusCode.Aborted, true)]
    [TestCase(StatusCode.PermissionDenied, false)]
    [TestCase(StatusCode.Unauthenticated, false)]
    [TestCase(StatusCode.Unimplemented, false)]
    [TestCase(StatusCode.NotFound, false)]
    [TestCase(StatusCode.FailedPrecondition, false)]
    [TestCase(StatusCode.ResourceExhausted, false)]
    [TestCase(StatusCode.InvalidArgument, false)]
    [TestCase(StatusCode.Cancelled, false)]
    [TestCase(StatusCode.Unknown, false)]
    public void A_status_code_decides_whether_or_not_a_wrapper_calls_it_transient(StatusCode code, bool transient)
    {
        var rpc = new RpcException(new Status(code, "x"));

        Assert.Multiple(() =>
        {
            Assert.That(AreaProbeFaults.IsTransient(rpc), Is.EqualTo(transient), "bare");
            Assert.That(AreaProbeFaults.IsTransient(new ShellTransportException("x", isTransient: !transient, rpc)), Is.EqualTo(transient), "under the shell transport");
            Assert.That(AreaProbeFaults.IsTransient(new LatticeStateApiException("x", rpc) { IsTransient = !transient }), Is.EqualTo(transient), "under the state API");
            Assert.That(AreaProbeFaults.IsTransient(new AggregateException(rpc)), Is.EqualTo(transient), "inside an aggregate");
        });
    }

    [Test]
    public void A_denial_anywhere_in_the_chain_is_never_transient()
    {
        var internalFault = new RpcException(new Status(StatusCode.Internal, "x"));

        Assert.Multiple(() =>
        {
            Assert.That(AreaProbeFaults.IsTransient(new LatticeAuthorizationDeniedException("denied")), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new UnauthorizedAccessException("denied", internalFault)), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new LatticeStateApiException("denied", internalFault) { IsPermissionDenied = true, IsTransient = true }), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new LatticeStateApiException("sign in", internalFault) { RequiresAuthentication = true, IsTransient = true }), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new HttpRequestException("forbidden", null, HttpStatusCode.Forbidden)), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new HttpRequestException("unauthorized", null, HttpStatusCode.Unauthorized)), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new AggregateException(internalFault, new LatticeAuthorizationDeniedException("denied"))), Is.False);
        });
    }

    [Test]
    public void Transport_faults_with_no_status_are_transient()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AreaProbeFaults.IsTransient(new TimeoutException()), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new HttpRequestException("reset")), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new IOException("reset")), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new System.Net.Sockets.SocketException()), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new InvalidOperationException("x", new TimeoutException())), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new ShellTransportException("down", isTransient: true, new InvalidOperationException())), Is.True);
            Assert.That(AreaProbeFaults.IsTransient(new LatticeStateApiException("x") { IsTransient = true }), Is.True);
        });
    }

    [Test]
    public void Anything_unrecognised_fails_closed_as_definite()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AreaProbeFaults.IsTransient(null), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new InvalidOperationException("not configured")), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new NotSupportedException()), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new OperationCanceledException()), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new ShellTransportException("bad", isTransient: false, new InvalidOperationException())), Is.False);
            Assert.That(AreaProbeFaults.IsTransient(new LatticeStateApiException("Not connected.")), Is.False);
            Assert.That(
                AreaProbeFaults.IsTransient(new LatticeStateApiException("cancelled", new OperationCanceledException()) { IsTransient = true }),
                Is.False,
                "a cancellation is not the cluster failing to answer");
        });
    }
}
