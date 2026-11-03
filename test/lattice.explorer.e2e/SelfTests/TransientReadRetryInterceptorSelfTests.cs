using Grpc.Core;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The contracts' read-retry policy (#5742): a side-effect-free read is retried on a transient
/// status, including the outcome-unknown <see cref="StatusCode.DeadlineExceeded"/>; a write is
/// never retried, because a write that timed out may already have applied; and a server fault is
/// never mistaken for a transient one.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class TransientReadRetryInterceptorSelfTests
{
    /// <summary>A read is retried on every transient status.</summary>
    /// <param name="method">A read method.</param>
    /// <param name="code">A transient status.</param>
    [Test]
    public void A_read_is_retried_on_a_transient_status(
        [Values("GetGroup", "ListRules", "ReadRange", "GetViewStatus")] string method,
        [Values(StatusCode.Unavailable, StatusCode.DeadlineExceeded, StatusCode.ResourceExhausted)] StatusCode code)
    {
        Assert.That(TransientReadRetryInterceptor.ShouldRetry(method, code), Is.True);
    }

    /// <summary>A write is never retried, not even when its outcome is unknown.</summary>
    /// <param name="method">A write method.</param>
    /// <param name="code">Any transient status.</param>
    [Test]
    public void A_write_is_never_retried(
        [Values("SetMany", "RemoveMember", "DropView", "DisableReplication", "CancelSchedule")] string method,
        [Values(StatusCode.Unavailable, StatusCode.DeadlineExceeded, StatusCode.ResourceExhausted)] StatusCode code)
    {
        Assert.That(TransientReadRetryInterceptor.ShouldRetry(method, code), Is.False);
    }

    /// <summary>A read that fails with a server fault or a refusal is not retried.</summary>
    /// <param name="code">A non-transient status.</param>
    [Test]
    public void A_read_is_not_retried_on_a_non_transient_status(
        [Values(StatusCode.Internal, StatusCode.PermissionDenied, StatusCode.InvalidArgument, StatusCode.NotFound, StatusCode.FailedPrecondition)] StatusCode code)
    {
        Assert.That(TransientReadRetryInterceptor.ShouldRetry("GetGroup", code), Is.False);
    }
}
