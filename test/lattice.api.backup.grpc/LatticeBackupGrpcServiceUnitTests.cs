using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice;
using Orleans.Lattice.Backup;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for the server-side <see cref="LatticeBackupGrpcService"/> driven
/// directly against a substitute <see cref="ILatticeBackupControl"/> facade and an
/// in-process <see cref="FakeServerCallContext"/>, with no live gRPC transport.
/// Focuses on the health RPCs, the credential-stamping seam, and the exception
/// mapping onto gRPC status codes in the shared <c>InvokeAsync</c> helper and the
/// two streaming RPCs; the success round trips are covered by the E2E suite.
/// </summary>
[TestFixture]
public sealed class LatticeBackupGrpcServiceUnitTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeBackupGrpcService CreateService(
        ILatticeBackupControl control,
        ILatticeBackupApiCredentialBridge? bridge = null)
    {
        var methods = LatticeBackupGrpcMethods.FromServiceProvider(_services);
        bridge ??= Substitute.For<ILatticeBackupApiCredentialBridge>();
        var schemeSource = Substitute.For<ILatticeBackupApiAuthSchemeSource>();
        return new LatticeBackupGrpcService(
            methods,
            control,
            bridge,
            schemeSource,
            Substitute.For<ILogger<LatticeBackupGrpcService>>());
    }

    private static FakeServerCallContext Context(string method = "unit") => new(method);

    private static BackupHealthReport Report(string backupId) =>
        new(
            backupId,
            BackupHealthStatus.Healthy,
            manifestPresent: true,
            Array.Empty<string>(),
            Array.Empty<string>(),
            DateTimeOffset.UtcNow,
            "ok");

    [Test]
    public async Task IsHealthMonitoringAvailable_maps_the_facade_flag_onto_the_response()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.IsHealthMonitoringAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        var service = CreateService(control);

        var response = await service.IsHealthMonitoringAvailable(new BackupHealthAvailabilityRequest(), Context());

        Assert.That(response.Available, Is.True);
    }

    [Test]
    public async Task CheckBackupHealth_wraps_the_fresh_report_as_found()
    {
        var report = Report("b1");
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync("b1", Arg.Any<CancellationToken>()).Returns(report);
        var service = CreateService(control);

        var response = await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b1" },
            Context());

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.True);
            Assert.That(response.Report, Is.SameAs(report));
        });
    }

    [Test]
    public async Task GetBackupHealth_wraps_a_stored_report_as_found()
    {
        var report = Report("b2");
        var control = Substitute.For<ILatticeBackupControl>();
        control.GetBackupHealthAsync("b2", Arg.Any<CancellationToken>()).Returns(report);
        var service = CreateService(control);

        var response = await service.GetBackupHealth(
            new BackupHealthGetRequestMessage { BackupId = "b2" },
            Context());

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.True);
            Assert.That(response.Report, Is.SameAs(report));
        });
    }

    [Test]
    public async Task GetBackupHealth_reports_not_found_when_no_report_exists()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.GetBackupHealthAsync("b3", Arg.Any<CancellationToken>()).Returns((BackupHealthReport?)null);
        var service = CreateService(control);

        var response = await service.GetBackupHealth(
            new BackupHealthGetRequestMessage { BackupId = "b3" },
            Context());

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.False);
            Assert.That(response.Report, Is.Null);
        });
    }

    [Test]
    public async Task ConfigureBackupHealth_forwards_the_config_to_the_facade()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        var service = CreateService(control);

        await service.ConfigureBackupHealth(
            new BackupHealthConfigureRequestMessage
            {
                BackupId = "b4",
                MonitoringEnabled = true,
                IntervalTicks = TimeSpan.FromHours(3).Ticks,
            },
            Context());

        await control.Received(1).ConfigureBackupHealthAsync(
            "b4",
            Arg.Is<BackupHealthConfig>(c => c.MonitoringEnabled && c.Interval == TimeSpan.FromHours(3)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InvokeAsync_stamps_the_bridged_credential_for_the_call()
    {
        var report = Report("b5");
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync("b5", Arg.Any<CancellationToken>()).Returns(report);
        var bridge = new HeaderLatticeBackupApiCredentialBridge(
            Options.Create(new LatticeBackupApiGrpcOptions()));
        var service = CreateService(control, bridge);
        var headers = new global::Grpc.Core.Metadata { { "authorization", "Bearer token-xyz" } };
        var context = new FakeServerCallContext("unit", headers);

        var response = await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b5" },
            context);

        Assert.That(response.Report, Is.SameAs(report));
    }

    private static IEnumerable<TestCaseData> InvokeExceptionCases()
    {
        yield return new TestCaseData(new OperationCanceledException(), StatusCode.Cancelled).SetName("OperationCanceled_maps_to_Cancelled");
        yield return new TestCaseData(new KeyNotFoundException("nope"), StatusCode.NotFound).SetName("KeyNotFound_maps_to_NotFound");
        yield return new TestCaseData(new LatticeRestoreValidationException("bad"), StatusCode.FailedPrecondition).SetName("RestoreValidation_maps_to_FailedPrecondition");
        yield return new TestCaseData(new ArgumentException("arg"), StatusCode.InvalidArgument).SetName("Argument_maps_to_InvalidArgument");
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("denied"), StatusCode.PermissionDenied).SetName("AuthorizationDenied_maps_to_PermissionDenied");
        yield return new TestCaseData(new TimeoutException("The operation has timed out."), StatusCode.Unavailable).SetName("TransientReminderTimeout_maps_to_Unavailable");
        yield return new TestCaseData(new InvalidOperationException("Reminder Service is still initializing and it is taking a long time. Please retry again later."), StatusCode.Unavailable).SetName("ReminderStillInitializing_maps_to_Unavailable");
        yield return new TestCaseData(new InvalidTimeZoneException("boom"), StatusCode.Internal).SetName("Unexpected_maps_to_Internal");
    }

    [TestCaseSource(nameof(InvokeExceptionCases))]
    public void InvokeAsync_maps_facade_exceptions_onto_status_codes(Exception thrown, StatusCode expected)
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(thrown);
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b" },
            Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(expected));
    }

    [Test]
    public void InvokeAsync_attaches_a_correlation_ref_to_the_opaque_internal_detail()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidTimeZoneException("secret internal detail"));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b" },
            Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(ex.Status.Detail, Does.Contain("ref:"),
                "The opaque Internal detail must carry a correlation id an operator can tie to the logged exception.");
            Assert.That(ex.Status.Detail, Does.Not.Contain("secret internal detail"),
                "The surfaced message must not leak the internal exception detail.");
        });
    }

    [Test]
    public void InvokeAsync_surfaces_a_transient_reminder_failure_as_retryable_unavailable()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("The operation has timed out."));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b" },
            Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unavailable));
            Assert.That(ex.Status.Detail, Does.Contain("ref:"));
        });
    }

    [Test]
    public void InvokeAsync_rethrows_an_rpc_exception_unwrapped()
    {
        var original = new RpcException(new Status(StatusCode.AlreadyExists, "dup"));
        var control = Substitute.For<ILatticeBackupControl>();
        control.CheckBackupHealthAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(original);
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.CheckBackupHealth(
            new BackupHealthCheckRequestMessage { BackupId = "b" },
            Context()));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public async Task ExportArtifact_streams_each_facade_chunk_onto_the_response()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ExportArtifactAsync("b", "a", Arg.Any<CancellationToken>())
            .Returns(Chunks(new byte[] { 1, 2 }, new byte[] { 3 }));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<ArtifactChunk>();

        await service.ExportArtifact(
            new ArtifactExportRequest { BackupId = "b", ArtifactId = "a" },
            writer,
            Context());

        Assert.Multiple(() =>
        {
            Assert.That(writer.Written, Has.Count.EqualTo(2));
            Assert.That(writer.Written[0].Data, Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(writer.Written[1].Data, Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public async Task ExportArtifact_swallows_cancellation_and_ends_the_stream_cleanly()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ExportArtifactAsync("b", "a", Arg.Any<CancellationToken>())
            .Returns(ThrowingChunks(new OperationCanceledException()));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<ArtifactChunk>();

        await service.ExportArtifact(
            new ArtifactExportRequest { BackupId = "b", ArtifactId = "a" },
            writer,
            Context());

        Assert.That(writer.Written, Is.Empty);
    }

    private static IEnumerable<TestCaseData> ExportExceptionCases()
    {
        yield return new TestCaseData(new KeyNotFoundException("x"), StatusCode.NotFound).SetName("Export_KeyNotFound_maps_to_NotFound");
        yield return new TestCaseData(new ArgumentException("x"), StatusCode.InvalidArgument).SetName("Export_Argument_maps_to_InvalidArgument");
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("x"), StatusCode.PermissionDenied).SetName("Export_AuthorizationDenied_maps_to_PermissionDenied");
        yield return new TestCaseData(new InvalidTimeZoneException("x"), StatusCode.Internal).SetName("Export_Unexpected_maps_to_Internal");
    }

    [TestCaseSource(nameof(ExportExceptionCases))]
    public void ExportArtifact_maps_facade_exceptions_onto_status_codes(Exception thrown, StatusCode expected)
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ExportArtifactAsync("b", "a", Arg.Any<CancellationToken>()).Returns(ThrowingChunks(thrown));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<ArtifactChunk>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ExportArtifact(
            new ArtifactExportRequest { BackupId = "b", ArtifactId = "a" },
            writer,
            Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(expected));
    }

    [Test]
    public void ExportArtifact_rethrows_an_rpc_exception_unwrapped()
    {
        var original = new RpcException(new Status(StatusCode.ResourceExhausted, "busy"));
        var control = Substitute.For<ILatticeBackupControl>();
        control.ExportArtifactAsync("b", "a", Arg.Any<CancellationToken>()).Returns(ThrowingChunks(original));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<ArtifactChunk>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ExportArtifact(
            new ArtifactExportRequest { BackupId = "b", ArtifactId = "a" },
            writer,
            Context()));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public async Task StreamBackups_swallows_cancellation_and_ends_the_stream_cleanly()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.StreamBackupsAsync(Arg.Any<CancellationToken>())
            .Returns(ThrowingManifests(new OperationCanceledException()));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<BackupManifest>();

        await service.StreamBackups(new BackupStreamRequest(), writer, Context());

        Assert.That(writer.Written, Is.Empty);
    }

    private static IEnumerable<TestCaseData> StreamExceptionCases()
    {
        yield return new TestCaseData(new ArgumentException("x"), StatusCode.InvalidArgument).SetName("Stream_Argument_maps_to_InvalidArgument");
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("x"), StatusCode.PermissionDenied).SetName("Stream_AuthorizationDenied_maps_to_PermissionDenied");
        yield return new TestCaseData(new InvalidTimeZoneException("x"), StatusCode.Internal).SetName("Stream_Unexpected_maps_to_Internal");
    }

    [TestCaseSource(nameof(StreamExceptionCases))]
    public void StreamBackups_maps_facade_exceptions_onto_status_codes(Exception thrown, StatusCode expected)
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.StreamBackupsAsync(Arg.Any<CancellationToken>()).Returns(ThrowingManifests(thrown));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<BackupManifest>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamBackups(
            new BackupStreamRequest(),
            writer,
            Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(expected));
    }

    [Test]
    public void StreamBackups_rethrows_an_rpc_exception_unwrapped()
    {
        var original = new RpcException(new Status(StatusCode.Unavailable, "down"));
        var control = Substitute.For<ILatticeBackupControl>();
        control.StreamBackupsAsync(Arg.Any<CancellationToken>()).Returns(ThrowingManifests(original));
        var service = CreateService(control);
        var writer = new RecordingServerStreamWriter<BackupManifest>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamBackups(
            new BackupStreamRequest(),
            writer,
            Context()));

        Assert.That(ex, Is.SameAs(original));
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(params byte[][] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ThrowingChunks(Exception ex)
    {
        await Task.CompletedTask;
        if (ex is not null)
        {
            throw ex;
        }

        yield break;
    }

    private static async IAsyncEnumerable<BackupManifest> ThrowingManifests(Exception ex)
    {
        await Task.CompletedTask;
        if (ex is not null)
        {
            throw ex;
        }

        yield break;
    }
}
