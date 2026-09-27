using Grpc.Core;
using NSubstitute;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

public sealed partial class AppsGrpcRoundTripTests
{
    [TestCase("argument", StatusCode.InvalidArgument)]
    [TestCase("missing", StatusCode.NotFound)]
    [TestCase("precondition", StatusCode.FailedPrecondition)]
    [TestCase("authorization", StatusCode.PermissionDenied)]
    [TestCase("tenant", StatusCode.PermissionDenied)]
    [TestCase("cancelled", StatusCode.Cancelled)]
    [TestCase("unexpected", StatusCode.Internal)]
    [TestCase("rpc", StatusCode.Internal)]
    public void Facade_errors_map_without_leaking_physical_tree_ids(string kind, StatusCode status)
    {
        const string secret = "t/private/a/demo/events";
        Exception failure = kind switch
        {
            "argument" => new ArgumentException(secret),
            "missing" => new KeyNotFoundException(secret),
            "precondition" => new InvalidOperationException(secret),
            "authorization" => new LatticeAuthorizationDeniedException(secret),
            "tenant" => new LatticeTenantAccessDeniedException(secret),
            "cancelled" => new OperationCanceledException(secret),
            "rpc" => new RpcException(new Status(StatusCode.Internal, secret)),
            _ => new Exception(secret),
        };
        _control.ListAsync(Arg.Any<CancellationToken>()).Returns(Task.FromException<AppCatalog>(failure));
        var error = Assert.ThrowsAsync<RpcException>(async () => await _client.ListAsync());
        Assert.That(error!.StatusCode, Is.EqualTo(status));
        Assert.That(error.Status.Detail, Does.Not.Contain(secret));
    }

    [Test]
    public async Task Credential_and_tenant_are_scoped_to_each_call_and_do_not_leak()
    {
        var observed = new List<(string? Token, string? Scheme, string? Tenant)>();
        _control.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed.Add((LatticeCredentialContext.Current?.Token, LatticeCredentialContext.Current?.Scheme,
                LatticeActiveTenantContext.Current?.Value));
            return new AppCatalog();
        });
        _headers.Add("authorization", "Bearer caller-token");
        _headers.Add("lattice-active-tenant", "tenant-one");
        await _client.ListAsync();
        _headers.Clear();
        await _client.ListAsync();
        Assert.That(observed, Is.EqualTo(new (string?, string?, string?)[]
            { ("caller-token", "Bearer", "tenant-one"), (null, null, null) }));
        Assert.That(LatticeCredentialContext.Current, Is.Null);
        Assert.That(LatticeActiveTenantContext.Current, Is.Null);
    }

    [Test]
    public async Task Cancellation_reaches_the_facade()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _control.ListAsync(Arg.Any<CancellationToken>()).Returns(async c =>
        {
            var ct = c.Arg<CancellationToken>();
            entered.SetResult();
            try { await Task.Delay(Timeout.Infinite, ct); }
            finally { cancelled.TrySetResult(); }
            return new AppCatalog();
        });
        using var cancellation = new CancellationTokenSource();
        var call = _client.ListAsync(cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancellation.Cancel();
        var error = Assert.ThrowsAsync<RpcException>(async () => await call);
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
