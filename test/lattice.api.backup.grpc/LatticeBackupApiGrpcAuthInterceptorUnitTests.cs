using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupApiGrpcAuthInterceptor"/> driven
/// directly - no live server. Exercises the enforcement-disabled short circuit,
/// the authorizer-cancellation mapping, the non-backup-service bypass on both the
/// unary and server-streaming handlers, and the internal operation/target
/// decoding, alongside the two shipped authorizers.
/// </summary>
[TestFixture]
public sealed class LatticeBackupApiGrpcAuthInterceptorUnitTests
{
    private static string FullMethod(string methodName) =>
        $"/{LatticeBackupGrpcMethods.ServiceName}/{methodName}";

    private static LatticeBackupApiGrpcAuthInterceptor Create(
        ILatticeBackupApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupApiGrpcOptions>>();
        monitor.CurrentValue.Returns(new LatticeBackupApiGrpcOptions { RequireAuthorization = requireAuthorization });
        return new LatticeBackupApiGrpcAuthInterceptor(
            authorizer,
            monitor,
            Substitute.For<ILogger<LatticeBackupApiGrpcAuthInterceptor>>());
    }

    [Test]
    public async Task UnaryServerHandler_when_authorization_disabled_skips_the_authorizer()
    {
        var authorizer = Substitute.For<ILatticeBackupApiAuthorizer>();
        var interceptor = Create(authorizer, requireAuthorization: false);
        var response = new BackupHealthAvailabilityResponse { Available = true };

        var result = await interceptor.UnaryServerHandler(
            new BackupHealthAvailabilityRequest(),
            new FakeServerCallContext(FullMethod(LatticeBackupGrpcMethods.ListBackupsMethodName)),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeBackupApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void UnaryServerHandler_maps_authorizer_cancellation_to_Cancelled()
    {
        var authorizer = Substitute.For<ILatticeBackupApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeBackupApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var interceptor = Create(authorizer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await interceptor.UnaryServerHandler(
            new BackupHealthAvailabilityRequest(),
            new FakeServerCallContext(FullMethod(LatticeBackupGrpcMethods.ListBackupsMethodName)),
            (_, _) => Task.FromResult(new BackupHealthAvailabilityResponse())));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public async Task UnaryServerHandler_non_backup_service_method_bypasses_enforcement()
    {
        var authorizer = Substitute.For<ILatticeBackupApiAuthorizer>();
        var interceptor = Create(authorizer);
        var response = new BackupHealthAvailabilityResponse();

        var result = await interceptor.UnaryServerHandler(
            new BackupHealthAvailabilityRequest(),
            new FakeServerCallContext("/some.other.Service/DoThing"),
            (_, _) => Task.FromResult(response));

        Assert.That(result, Is.SameAs(response));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeBackupApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ServerStreamingServerHandler_non_backup_service_method_invokes_the_continuation()
    {
        var authorizer = Substitute.For<ILatticeBackupApiAuthorizer>();
        var interceptor = Create(authorizer);
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            new BackupStreamRequest(),
            new RecordingServerStreamWriter<BackupManifest>(),
            new FakeServerCallContext("/some.other.Service/Stream"),
            (_, _, _) =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

        Assert.That(invoked, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeBackupApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ServerStreamingServerHandler_backup_method_enforces_and_admits_when_authorized()
    {
        var authorizer = Substitute.For<ILatticeBackupApiAuthorizer>();
        authorizer.IsAuthorizedAsync(Arg.Any<LatticeBackupApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(true);
        var interceptor = Create(authorizer);
        var invoked = false;

        await interceptor.ServerStreamingServerHandler(
            new BackupStreamRequest(),
            new RecordingServerStreamWriter<BackupManifest>(),
            new FakeServerCallContext(FullMethod(LatticeBackupGrpcMethods.StreamBackupsMethodName)),
            (_, _, _) =>
            {
                invoked = true;
                return Task.CompletedTask;
            });

        Assert.That(invoked, Is.True);
    }

    private static IEnumerable<TestCaseData> DescribeCallCases()
    {
        yield return new TestCaseData(
            LatticeBackupGrpcMethods.IsHealthMonitoringAvailableMethodName,
            (object)new BackupHealthAvailabilityRequest(),
            LatticeBackupApiOperation.IsHealthMonitoringAvailable,
            (string?)null).SetName("IsHealthMonitoringAvailable_no_target");
        yield return new TestCaseData(
            LatticeBackupGrpcMethods.CheckBackupHealthMethodName,
            (object)new BackupHealthCheckRequestMessage { BackupId = "b-check" },
            LatticeBackupApiOperation.CheckBackupHealth,
            (string?)"b-check").SetName("CheckBackupHealth_targets_backup");
        yield return new TestCaseData(
            LatticeBackupGrpcMethods.GetBackupHealthMethodName,
            (object)new BackupHealthGetRequestMessage { BackupId = "b-get" },
            LatticeBackupApiOperation.GetBackupHealth,
            (string?)"b-get").SetName("GetBackupHealth_targets_backup");
        yield return new TestCaseData(
            LatticeBackupGrpcMethods.ConfigureBackupHealthMethodName,
            (object)new BackupHealthConfigureRequestMessage { BackupId = "b-cfg" },
            LatticeBackupApiOperation.ConfigureBackupHealth,
            (string?)"b-cfg").SetName("ConfigureBackupHealth_targets_backup");
        yield return new TestCaseData(
            "SomeFutureRpc",
            (object)new BackupHealthAvailabilityRequest(),
            LatticeBackupApiOperation.Unknown,
            (string?)null).SetName("Unrecognised_method_maps_to_Unknown");
    }

    [TestCaseSource(nameof(DescribeCallCases))]
    public void DescribeCall_decodes_operation_and_target(
        string methodName,
        object request,
        LatticeBackupApiOperation expectedOperation,
        string? expectedTarget)
    {
        var (operation, targetId) = LatticeBackupApiGrpcAuthInterceptor.DescribeCall(FullMethod(methodName), request);

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(expectedOperation));
            Assert.That(targetId, Is.EqualTo(expectedTarget));
        });
    }

    [Test]
    public void IsUnauthenticatedMethod_is_true_only_for_the_auth_scheme_discovery_rpc()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeBackupApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    FullMethod(LatticeBackupGrpcMethods.GetAuthSchemeMethodName)),
                Is.True);
            Assert.That(
                LatticeBackupApiGrpcAuthInterceptor.IsUnauthenticatedMethod(
                    FullMethod(LatticeBackupGrpcMethods.ListBackupsMethodName)),
                Is.False);
        });
    }

    [Test]
    public async Task DenyAllBackupApiAuthorizer_rejects_every_call()
    {
        var authorizer = new DenyAllBackupApiAuthorizer();

        var allowed = await authorizer.IsAuthorizedAsync(
            new LatticeBackupApiAuthorizationContext(
                new FakeServerCallContext(FullMethod(LatticeBackupGrpcMethods.ListBackupsMethodName)),
                LatticeBackupApiOperation.ListBackups,
                targetId: null),
            CancellationToken.None);

        Assert.That(allowed, Is.False);
    }

    [Test]
    public async Task AllowAllBackupApiAuthorizer_permits_every_call()
    {
        var authorizer = new AllowAllBackupApiAuthorizer();

        var allowed = await authorizer.IsAuthorizedAsync(
            new LatticeBackupApiAuthorizationContext(
                new FakeServerCallContext(FullMethod(LatticeBackupGrpcMethods.ListBackupsMethodName)),
                LatticeBackupApiOperation.ListBackups,
                targetId: null),
            CancellationToken.None);

        Assert.That(allowed, Is.True);
    }
}
