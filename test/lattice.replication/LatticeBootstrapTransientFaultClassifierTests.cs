using System.Net.Http;
using System.Net.Sockets;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of <see cref="LatticeBootstrapTransientFaultClassifier.IsTransient(Exception)"/>:
/// asserts the canonical transient-fault types are classified true,
/// non-transient types are classified false, and the reflection-based
/// gRPC matching path works for stub <c>RpcException</c>-shaped types.
/// </summary>
[TestFixture]
public sealed class LatticeBootstrapTransientFaultClassifierTests
{
    [Test]
    public void IsTransient_returns_true_for_TimeoutException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new TimeoutException("boom")), Is.True);

    [Test]
    public void IsTransient_returns_true_for_HttpRequestException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new HttpRequestException("boom")), Is.True);

    [Test]
    public void IsTransient_returns_true_for_SocketException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new SocketException()), Is.True);

    [Test]
    public void IsTransient_returns_true_for_IOException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new IOException("boom")), Is.True);

    [Test]
    public void IsTransient_returns_false_for_InvalidOperationException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new InvalidOperationException("boom")), Is.False);

    [Test]
    public void IsTransient_returns_false_for_ArgumentException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new ArgumentException("boom")), Is.False);

    [Test]
    public void IsTransient_returns_false_for_NullReferenceException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new NullReferenceException("boom")), Is.False);

    [Test]
    public void IsTransient_returns_false_for_OperationCanceledException()
        => Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new OperationCanceledException("boom")), Is.False);

    [Test]
    public void IsTransient_unwraps_AggregateException_to_inner_classifier_decision()
    {
        // AggregateException wrapping a transient inner should be classified transient.
        var transient = new AggregateException(new TimeoutException("inner"));
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(transient), Is.True);

        // AggregateException wrapping a non-transient inner should be classified non-transient.
        var permanent = new AggregateException(new InvalidOperationException("inner"));
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(permanent), Is.False);
    }

    [Test]
    public void IsTransient_throws_when_exception_argument_is_null()
        => Assert.That(
            () => LatticeBootstrapTransientFaultClassifier.IsTransient(null!),
            Throws.InstanceOf<ArgumentNullException>());

    /// <summary>
    /// Test-local stub that mimics the shape of
    /// <c>Grpc.Core.RpcException</c> for the reflection-based status
    /// code extraction path: a fully-qualified type name of
    /// <c>Grpc.Core.RpcException</c> and an integer
    /// <c>StatusCode</c> property. Keeps the test free of a real
    /// gRPC dependency while still exercising the classifier's
    /// reflection branch end-to-end.
    /// </summary>
    [Test]
    public void IsTransient_returns_true_for_grpc_status_code_unavailable()
    {
        var ex = StubRpcException.Create(statusCode: 14, message: "Unavailable");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.True);
    }

    [Test]
    public void IsTransient_returns_true_for_grpc_status_code_deadline_exceeded()
    {
        var ex = StubRpcException.Create(statusCode: 4, message: "DeadlineExceeded");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.True);
    }

    [Test]
    public void IsTransient_returns_true_for_grpc_status_code_aborted()
    {
        var ex = StubRpcException.Create(statusCode: 10, message: "Aborted");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.True);
    }

    [Test]
    public void IsTransient_returns_false_for_grpc_status_code_invalid_argument()
    {
        var ex = StubRpcException.Create(statusCode: 3, message: "InvalidArgument");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.False);
    }

    [Test]
    public void IsTransient_returns_false_for_grpc_status_code_unimplemented()
    {
        var ex = StubRpcException.Create(statusCode: 12, message: "Unimplemented");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.False);
    }

    /// <summary>
    /// Asserts the by-name match for Orleans' cross-grain
    /// <c>EnumerationAbortedException</c> classifies as transient. The
    /// receiver-side bootstrap drain reads the producer's snapshot
    /// stream through a cross-grain <c>IAsyncEnumerable</c>; a heavy
    /// producer-side workload can cause the Orleans-managed
    /// enumerator session to expire mid-drain, and the receiver's
    /// retry path reopens the stream from <c>LastAppliedHlc</c> so
    /// resuming after such an expiry is correctness-preserving.
    /// </summary>
    [Test]
    public void IsTransient_returns_true_for_orleans_enumeration_aborted()
    {
        var ex = new Orleans.Runtime.EnumerationAbortedException(
            "Enumeration aborted: the remote target does not have a record of this enumerator.");
        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(ex), Is.True);
    }
}
