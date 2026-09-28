using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests.Bridge;

/// <summary>
/// Default-deny, classification, registration and failure-mapping coverage for the app bridge gRPC binding.
/// </summary>
[TestFixture]
public sealed class AppBridgeGrpcSecurityTests
{
    private static readonly AppBridgeTarget Target = new() { AppSlug = "crm", InstallRevision = 7, LogicalTree = "notes" };

    [TestCase("Get")]
    [TestCase("Scan")]
    [TestCase("Set")]
    [TestCase("Delete")]
    public async Task Registration_is_default_deny_for_every_bridge_method(string method)
    {
        using var services = new ServiceCollection().AddLogging().AddLatticeAppBridgeApiGrpc().BuildServiceProvider();
        var authorizer = services.GetRequiredService<ILatticeAppsApiAuthorizer>();
        Assert.That(authorizer, Is.TypeOf<DenyAppsApiAuthorizer>());
        Assert.That(await authorizer.IsAuthorizedAsync(default, default), Is.False);

        var interceptor = services.GetRequiredService<LatticeAppsApiGrpcAuthInterceptor>();
        var reached = false;
        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            Request(method), new TestCallContext(LatticeAppBridgeGrpcMethods.ServicePrefix + method),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult(new AppsEmptyRequest());
            }));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(reached, Is.False);
    }

    [Test]
    public void Registration_is_idempotent_and_keeps_a_custom_authorizer()
    {
        var custom = Substitute.For<ILatticeAppsApiAuthorizer>();
        var services = new ServiceCollection().AddLogging().AddSingleton(custom);

        services.AddLatticeAppBridgeApiGrpc().AddLatticeAppBridgeApiGrpc();

        Assert.That(services.Count(d => d.ServiceType == typeof(LatticeAppBridgeGrpcService)), Is.EqualTo(1));
        Assert.That(services.Count(d => d.ServiceType == typeof(LatticeAppBridgeGrpcMethods)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeAppsApiAuthorizer>(), Is.SameAs(custom));
        var options = provider.GetRequiredService<IOptions<global::Grpc.AspNetCore.Server.GrpcServiceOptions<LatticeAppBridgeGrpcService>>>().Value;
        Assert.That(options.Interceptors.Count(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)), Is.EqualTo(1));
    }

    [Test]
    public void Registration_and_mapping_reject_null_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppBridgeApiGrpc());
        Assert.Throws<ArgumentNullException>(() => ((Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)null!).MapLatticeAppBridgeApiGrpc());
    }

    [Test]
    public void Every_bridge_method_is_classified_from_its_bound_method_and_request_shape()
    {
        var cases = new (string Method, object Request, LatticeAppsApiOperation Operation)[]
        {
            ("Get", Request("Get"), LatticeAppsApiOperation.BridgeGet),
            ("Scan", Request("Scan"), LatticeAppsApiOperation.BridgeScan),
            ("Set", Request("Set"), LatticeAppsApiOperation.BridgeSet),
            ("Delete", Request("Delete"), LatticeAppsApiOperation.BridgeDelete),
        };

        foreach (var (method, request, operation) in cases)
        {
            Assert.That(
                LatticeAppsApiGrpcAuthInterceptor.DescribeCall(LatticeAppBridgeGrpcMethods.ServicePrefix + method, request),
                Is.EqualTo((operation, (string?)"crm")),
                method);
        }
    }

    [Test]
    public void A_request_with_no_target_is_classified_without_a_slug()
    {
        var request = new AppsBridgeKeyRequest { Target = null!, Key = "k" };

        Assert.That(LatticeAppsApiGrpcAuthInterceptor.DescribeCall(LatticeAppBridgeGrpcMethods.ServicePrefix + "Get", request),
            Is.EqualTo((LatticeAppsApiOperation.BridgeGet, (string?)null)));
    }

    [TestCase("Get", "Scan")]
    [TestCase("Scan", "Get")]
    [TestCase("Set", "Delete")]
    [TestCase("Delete", "Set")]
    [TestCase("Unknown", "Get")]
    public void A_mismatched_or_unknown_request_fails_closed_even_when_enforcement_is_disabled(string method, string shape)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAppsApiGrpcOptions { RequireAuthorization = false });
        var interceptor = new LatticeAppsApiGrpcAuthInterceptor(
            Substitute.For<ILatticeAppsApiAuthorizer>(), options, NullLogger<LatticeAppsApiGrpcAuthInterceptor>.Instance);

        var error = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            Request(shape), new TestCallContext(LatticeAppBridgeGrpcMethods.ServicePrefix + method),
            (_, _) => Task.FromResult(new AppsEmptyRequest())));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Streaming_calls_to_the_bridge_service_are_refused()
    {
        var options = Substitute.For<IOptionsMonitor<LatticeAppsApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeAppsApiGrpcOptions());
        var interceptor = new LatticeAppsApiGrpcAuthInterceptor(
            Substitute.For<ILatticeAppsApiAuthorizer>(), options, NullLogger<LatticeAppsApiGrpcAuthInterceptor>.Instance);
        var context = new TestCallContext(LatticeAppBridgeGrpcMethods.ServicePrefix + "Get");

        var error = Assert.Throws<RpcException>(() => interceptor.ServerStreamingServerHandler<AppsBridgeKeyRequest, AppsBridgeGetResponse>(
            (AppsBridgeKeyRequest)Request("Get"), null!, context, (_, _, _) => Task.CompletedTask));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [TestCase(AppBridgeFailure.Denied, StatusCode.PermissionDenied)]
    [TestCase(AppBridgeFailure.NotFound, StatusCode.NotFound)]
    [TestCase(AppBridgeFailure.Invalid, StatusCode.InvalidArgument)]
    [TestCase(AppBridgeFailure.TooLarge, StatusCode.ResourceExhausted)]
    [TestCase(AppBridgeFailure.Conflict, StatusCode.Aborted)]
    [TestCase(AppBridgeFailure.Unavailable, StatusCode.Unavailable)]
    public void Failure_codes_map_one_to_one_onto_status_codes(AppBridgeFailure failure, StatusCode code)
    {
        Assert.That(AppBridgeGrpcStatus.ToStatusCode(failure), Is.EqualTo(code));
        Assert.That(AppBridgeGrpcStatus.ToFailure(code), Is.EqualTo(failure));
        var status = AppBridgeGrpcStatus.ToStatus(failure);
        Assert.That(status.StatusCode, Is.EqualTo(code));
        Assert.That(status.Detail, Is.EqualTo(AppBridgeException.DefaultMessage(failure)));
    }

    [Test]
    public void An_undefined_failure_code_is_sent_as_a_denial()
    {
        Assert.That(AppBridgeGrpcStatus.ToStatusCode((AppBridgeFailure)99), Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(AppBridgeGrpcStatus.ToStatus((AppBridgeFailure)99).Detail, Is.EqualTo(AppBridgeException.DefaultMessage(AppBridgeFailure.Denied)));
    }

    [TestCase(StatusCode.Unauthenticated, AppBridgeFailure.Denied)]
    [TestCase(StatusCode.OutOfRange, AppBridgeFailure.TooLarge)]
    [TestCase(StatusCode.FailedPrecondition, AppBridgeFailure.Conflict)]
    [TestCase(StatusCode.Internal, AppBridgeFailure.Unavailable)]
    [TestCase(StatusCode.DeadlineExceeded, AppBridgeFailure.Unavailable)]
    [TestCase(StatusCode.Unknown, AppBridgeFailure.Unavailable)]
    [TestCase(StatusCode.Cancelled, AppBridgeFailure.Unavailable)]
    public void Status_codes_the_service_never_sends_for_a_failure_are_mapped_conservatively(StatusCode code, AppBridgeFailure expected) =>
        Assert.That(AppBridgeGrpcStatus.ToFailure(code), Is.EqualTo(expected));

    [Test]
    public void The_bridge_bounds_hold_the_largest_value_and_the_response_budget()
    {
        Assert.That(LatticeAppsGrpcMarshallers.MaxBridgeRequestBytes, Is.InRange(64 * 1024, (64 * 1024) + (16 * 1024)));
        Assert.That(LatticeAppsGrpcMarshallers.MaxBridgeResponseBytes, Is.InRange(1024 * 1024, (1024 * 1024) + (128 * 1024)));
    }

    private static object Request(string method) => method switch
    {
        "Get" or "Delete" => new AppsBridgeKeyRequest { Target = Target, Key = "k" },
        "Scan" => new AppsBridgeScanRequest { Target = Target, Prefix = string.Empty, PageSize = 10 },
        "Set" => new AppsBridgeSetRequest { Target = Target, Key = "k", Value = new byte[] { 1 } },
        _ => throw new ArgumentException(method),
    };
}
