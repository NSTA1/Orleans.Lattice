using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpDiscoveryFaultClassifier"/>, which decides
/// whether a discovery fault means "the backend never answered" (transient, and so a
/// permission set must not be advertised as if it were authoritative) or "the backend
/// answered, and the answer denies" (fail closed, as before).
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpDiscoveryFaultClassifierTests
{
    [TestCase(StatusCode.Cancelled)]
    [TestCase(StatusCode.DeadlineExceeded)]
    [TestCase(StatusCode.Unavailable)]
    [TestCase(StatusCode.Internal)]
    [TestCase(StatusCode.ResourceExhausted)]
    [TestCase(StatusCode.Aborted)]
    public void A_no_answer_status_is_transient(StatusCode status)
    {
        Assert.That(
            LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(
                new RpcException(new Status(status, "no answer"))),
            Is.True);
    }

    [TestCase(StatusCode.PermissionDenied)]
    [TestCase(StatusCode.Unauthenticated)]
    [TestCase(StatusCode.NotFound)]
    [TestCase(StatusCode.InvalidArgument)]
    [TestCase(StatusCode.FailedPrecondition)]
    [TestCase(StatusCode.OutOfRange)]
    [TestCase(StatusCode.AlreadyExists)]
    [TestCase(StatusCode.Unimplemented)]
    [TestCase(StatusCode.OK)]
    public void An_authoritative_status_is_not_transient(StatusCode status)
    {
        Assert.That(
            LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(
                new RpcException(new Status(status, "answered"))),
            Is.False);
    }

    [Test]
    public void An_orleans_response_deadline_is_transient()
    {
        Assert.That(
            LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(
                new TimeoutException("Response did not arrive on response id 7.")),
            Is.True);
    }

    [Test]
    public void A_plain_application_fault_is_not_transient()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(
                    new InvalidOperationException("boom")),
                Is.False);
            Assert.That(
                LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(
                    new UnauthorizedAccessException("denied")),
                Is.False);
        });
    }

    [Test]
    public void Null_is_not_transient()
    {
        Assert.That(LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(null), Is.False);
    }

    [Test]
    public void A_wrapped_transient_fault_is_unwrapped()
    {
        var nested = new InvalidOperationException(
            "resolving facade access failed",
            new RpcException(new Status(StatusCode.Cancelled, "cancelled")));

        Assert.That(LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(nested), Is.True);
    }

    [Test]
    public void An_aggregate_carrying_a_transient_fault_is_unwrapped()
    {
        var aggregate = new AggregateException(
            new InvalidOperationException("unrelated"),
            new RpcException(new Status(StatusCode.Unavailable, "silo down")));

        Assert.That(LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(aggregate), Is.True);
    }

    [Test]
    public void An_aggregate_of_only_authoritative_faults_is_not_transient()
    {
        var aggregate = new AggregateException(
            new InvalidOperationException("unrelated"),
            new RpcException(new Status(StatusCode.PermissionDenied, "denied")));

        Assert.That(LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(aggregate), Is.False);
    }

    [Test]
    public void A_silo_churn_fault_is_transient_by_type_name()
    {
        Assert.That(
            LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(new SiloUnavailableException("churn")),
            Is.True);
    }

    /// <summary>
    /// A stand-in for the Orleans runtime type of the same name, which is not part of
    /// this project's reference closure. The classifier matches it by type name, so the
    /// stand-in exercises exactly the production code path.
    /// </summary>
    private sealed class SiloUnavailableException(string message) : Exception(message);
}
