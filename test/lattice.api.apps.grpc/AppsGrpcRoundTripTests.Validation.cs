using Grpc.Core;
using NSubstitute;
using NSubstitute.Extensions;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

public sealed partial class AppsGrpcRoundTripTests
{
    [TestCase(null)]
    [TestCase("")]
    [TestCase(" ")]
    public void Client_rejects_missing_slugs_before_dispatch(string? slug)
    {
        Assert.CatchAsync<ArgumentException>(async () => await _client.EnableAsync(slug!));
        Assert.CatchAsync<ArgumentException>(async () => await _client.DisableAsync(slug!));
        Assert.CatchAsync<ArgumentException>(async () => await _client.UninstallAsync(slug!));
        Assert.CatchAsync<ArgumentException>(async () => await _client.DescribeAsync(slug!));
        Assert.CatchAsync<ArgumentException>(async () => await _client.GetConsentAsync(slug!));
        Assert.That(_control.ReceivedCalls(), Is.Empty);
        Assert.That(_authorizer.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Client_rejects_null_requests_and_dependencies()
    {
        Assert.Throws<ArgumentNullException>(() => LatticeAppsApiGrpcClient.Create(null!, _serializers));
        Assert.Throws<ArgumentNullException>(() => LatticeAppsApiGrpcClient.Create(_channel.CreateCallInvoker(), null!));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await _client.InstallAsync(null!));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await _client.UpdateConsentAsync(null!));
        Assert.That(_control.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void Authorizer_failures_are_sanitized_and_do_not_reach_the_facade()
    {
        _authorizer.Configure().IsAuthorizedAsync(
            Arg.Any<LatticeAppsApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<bool>(new RpcException(new Status(StatusCode.PermissionDenied, "private-tree"))));
        var error = Assert.ThrowsAsync<RpcException>(async () => await _client.ListAsync());
        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.Internal));
        Assert.That(error.Status.Detail, Does.Not.Contain("private-tree"));
        Assert.That(_control.ReceivedCalls(), Is.Empty);
    }
}
