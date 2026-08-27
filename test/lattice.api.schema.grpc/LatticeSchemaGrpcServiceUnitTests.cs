using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Direct unit tests for the server-side <see cref="LatticeSchemaGrpcService"/>,
/// exercised against a substituted <see cref="ILatticeSchemaControl"/> facade and
/// a <see cref="FakeServerCallContext"/> - with no Orleans cluster or gRPC server.
/// Covers each RPC's mapping of a facade result onto its wire response, the
/// caller-credential bridging scope, and the exhaustive translation of typed
/// facade faults onto gRPC status codes (the <c>InvokeAsync</c> and
/// <c>StreamDeadLetters</c> catch ladders). The live transport is covered by the
/// integration-tagged E2E fixture; this fixture covers the adapter logic cheaply.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaGrpcServiceUnitTests
{
    private const string Tree = "orders";

    private ServiceProvider _serializerProvider = null!;
    private LatticeSchemaGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeSchemaGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private static ILatticeSchemaApiCredentialBridge AnonymousBridge()
    {
        var bridge = Substitute.For<ILatticeSchemaApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return bridge;
    }

    private LatticeSchemaGrpcService CreateService(
        ILatticeSchemaControl control,
        ILatticeSchemaApiCredentialBridge? bridge = null,
        ILatticeSchemaApiAuthSchemeSource? authSchemeSource = null)
    {
        return new LatticeSchemaGrpcService(
            _methods,
            control,
            bridge ?? AnonymousBridge(),
            authSchemeSource ?? Substitute.For<ILatticeSchemaApiAuthSchemeSource>(),
            NullLogger<LatticeSchemaGrpcService>.Instance);
    }

    private static FakeServerCallContext Context(string methodName) =>
        new(SchemaGrpcTestDoubles.FullMethod(methodName));

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public void Constructor_null_argument_throws()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var bridge = AnonymousBridge();
        var source = Substitute.For<ILatticeSchemaApiAuthSchemeSource>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeSchemaGrpcService(null!, control, bridge, source, NullLogger<LatticeSchemaGrpcService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaGrpcService(_methods, null!, bridge, source, NullLogger<LatticeSchemaGrpcService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaGrpcService(_methods, control, null!, source, NullLogger<LatticeSchemaGrpcService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaGrpcService(_methods, control, bridge, null!, NullLogger<LatticeSchemaGrpcService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaGrpcService(_methods, control, bridge, source, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task SetPolicy_acks_and_delegates_to_the_facade()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var service = CreateService(control);

        var response = await service.SetPolicy(
            new SetPolicyRequest { TreeId = Tree, Policy = JsonPolicy() },
            Context(LatticeSchemaGrpcMethods.SetPolicyMethodName));

        Assert.That(response, Is.Not.Null);
        await control.Received(1).SetPolicyAsync(Tree, Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearPolicy_returns_the_removed_flag()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ClearPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns(true);
        var service = CreateService(control);

        var response = await service.ClearPolicy(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.ClearPolicyMethodName));

        Assert.That(response.Removed, Is.True);
    }

    [Test]
    public async Task GetPolicy_reports_found_when_a_policy_exists()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var service = CreateService(control);

        var response = await service.GetPolicy(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetPolicyMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.True);
            Assert.That(response.Policy, Is.Not.Null);
        });
    }

    [Test]
    public async Task GetPolicy_reports_not_found_when_no_policy_exists()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns((LatticeSchemaPolicy?)null);
        var service = CreateService(control);

        var response = await service.GetPolicy(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetPolicyMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.False);
            Assert.That(response.Policy, Is.Null);
        });
    }

    [Test]
    public async Task CountDeadLetters_returns_the_count()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.CountDeadLettersAsync(Tree, Arg.Any<CancellationToken>()).Returns(3);
        var service = CreateService(control);

        var response = await service.CountDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.CountDeadLettersMethodName));

        Assert.That(response.Count, Is.EqualTo(3));
    }

    [Test]
    public async Task SetVersionConfig_acks_and_delegates_to_the_facade()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var service = CreateService(control);

        var response = await service.SetVersionConfig(
            new SetVersionConfigRequest { TreeId = Tree, Config = new LatticeSchemaVersionConfig(1, 2) },
            Context(LatticeSchemaGrpcMethods.SetVersionConfigMethodName));

        Assert.That(response, Is.Not.Null);
        await control.Received(1).SetVersionConfigAsync(Tree, Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetVersionConfig_reports_found_when_versioned()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetVersionConfigAsync(Tree, Arg.Any<CancellationToken>()).Returns(new LatticeSchemaVersionConfig(1, 4));
        var service = CreateService(control);

        var response = await service.GetVersionConfig(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetVersionConfigMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.True);
            Assert.That(response.Config.TargetVersion, Is.EqualTo(4u));
        });
    }

    [Test]
    public async Task GetVersionConfig_reports_not_found_when_unversioned()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetVersionConfigAsync(Tree, Arg.Any<CancellationToken>()).Returns((LatticeSchemaVersionConfig?)null);
        var service = CreateService(control);

        var response = await service.GetVersionConfig(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetVersionConfigMethodName));

        Assert.That(response.Found, Is.False);
    }

    [Test]
    public async Task AdvanceTargetVersion_returns_the_updated_config()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.AdvanceTargetVersionAsync(Tree, 5, Arg.Any<CancellationToken>()).Returns(new LatticeSchemaVersionConfig(1, 5));
        var service = CreateService(control);

        var response = await service.AdvanceTargetVersion(
            new AdvanceVersionRequest { TreeId = Tree, NewTargetVersion = 5 },
            Context(LatticeSchemaGrpcMethods.AdvanceTargetVersionMethodName));

        Assert.That(response.Config.TargetVersion, Is.EqualTo(5u));
    }

    [Test]
    public async Task AdvanceAndMigrate_returns_the_report()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.AdvanceAndMigrateAsync(Tree, 6, Arg.Any<CancellationToken>()).Returns(LatticeSchemaRemediationReport.Idle);
        var service = CreateService(control);

        var response = await service.AdvanceAndMigrate(
            new AdvanceVersionRequest { TreeId = Tree, NewTargetVersion = 6 },
            Context(LatticeSchemaGrpcMethods.AdvanceAndMigrateMethodName));

        Assert.That(response.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public async Task MigrateToTargetVersion_returns_the_report()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.MigrateToTargetVersionAsync(Tree, Arg.Any<CancellationToken>()).Returns(LatticeSchemaRemediationReport.Idle);
        var service = CreateService(control);

        var response = await service.MigrateToTargetVersion(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.MigrateToTargetVersionMethodName));

        Assert.That(response.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public async Task ClearVersionConfig_returns_the_removed_flag()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ClearVersionConfigAsync(Tree, Arg.Any<CancellationToken>()).Returns(true);
        var service = CreateService(control);

        var response = await service.ClearVersionConfig(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.ClearVersionConfigMethodName));

        Assert.That(response.Removed, Is.True);
    }

    [Test]
    public async Task Remediate_returns_the_report()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.RemediateAsync(Tree, Arg.Any<LatticeValueTransform>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaRemediationReport.Idle);
        var service = CreateService(control);

        var response = await service.Remediate(
            new RemediateRequest { TreeId = Tree, Transform = LatticeValueTransform.Passthrough(), TargetPolicy = JsonPolicy() },
            Context(LatticeSchemaGrpcMethods.RemediateMethodName));

        Assert.That(response.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public async Task GetRemediationStatus_returns_the_report()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetRemediationStatusAsync(Tree, Arg.Any<CancellationToken>()).Returns(LatticeSchemaRemediationReport.Idle);
        var service = CreateService(control);

        var response = await service.GetRemediationStatus(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetRemediationStatusMethodName));

        Assert.That(response.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public async Task ScanCompliance_returns_the_report()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ScanComplianceAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(LatticeSchemaComplianceReport.Ungoverned(Tree) with { HasPolicy = true });
        var service = CreateService(control);

        var response = await service.ScanCompliance(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.ScanComplianceMethodName));

        Assert.That(response.Report.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public async Task ProbeCapabilities_returns_the_capabilities()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaCapabilities { TreeId = Tree });
        var service = CreateService(control);

        var response = await service.ProbeCapabilities(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.ProbeCapabilitiesMethodName));

        Assert.That(response.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public async Task GetAuthScheme_returns_the_advertisement_without_bridging_a_credential()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var bridge = AnonymousBridge();
        var source = Substitute.For<ILatticeSchemaApiAuthSchemeSource>();
        source.GetAdvertisement().Returns(new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "basic" } },
        });
        var service = CreateService(control, bridge, source);

        var response = await service.GetAuthScheme(
            new AuthSchemeAdvertisementRequest(),
            Context(LatticeSchemaGrpcMethods.GetAuthSchemeMethodName));

        Assert.That(response.Schemes, Has.Count.EqualTo(1));
        bridge.DidNotReceive().Resolve(Arg.Any<ServerCallContext>());
    }

    [Test]
    public async Task Unary_call_bridges_a_resolved_caller_credential()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var bridge = Substitute.For<ILatticeSchemaApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns(new LatticeCredential("token", "Bearer"));
        var service = CreateService(control, bridge);

        await service.CountDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.CountDeadLettersMethodName));

        bridge.Received(1).Resolve(Arg.Any<ServerCallContext>());
    }

    [Test]
    public void Unary_call_with_null_request_throws()
    {
        var service = CreateService(Substitute.For<ILatticeSchemaControl>());
        Assert.That(
            async () => await service.GetPolicy(null!, Context(LatticeSchemaGrpcMethods.GetPolicyMethodName)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Unary_call_with_null_context_throws()
    {
        var service = CreateService(Substitute.For<ILatticeSchemaControl>());
        Assert.That(
            async () => await service.GetPolicy(new SchemaTreeRequest { TreeId = Tree }, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetAuthScheme_with_null_request_throws()
    {
        var service = CreateService(Substitute.For<ILatticeSchemaControl>());
        Assert.That(
            async () => await service.GetAuthScheme(null!, Context(LatticeSchemaGrpcMethods.GetAuthSchemeMethodName)),
            Throws.ArgumentNullException);
    }

    [TestCaseSource(nameof(UnaryFaultCases))]
    public async Task Unary_call_translates_facade_faults_to_status_codes(Exception thrown, StatusCode expected)
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).ThrowsAsync(thrown);
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetPolicy(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetPolicyMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(expected));
        await Task.CompletedTask;
    }

    private static IEnumerable<TestCaseData> UnaryFaultCases()
    {
        yield return new TestCaseData(new KeyNotFoundException("missing"), StatusCode.NotFound).SetName("KeyNotFound_to_NotFound");
        yield return new TestCaseData(new InvalidOperationException("precondition"), StatusCode.FailedPrecondition).SetName("InvalidOperation_to_FailedPrecondition");
        yield return new TestCaseData(
            new LatticeQuotaExceededException("quota", "orders", LatticeQuotaExceededException.KeysDimension, 12, 10),
            StatusCode.ResourceExhausted).SetName("QuotaExceeded_to_ResourceExhausted");
        yield return new TestCaseData(new ArgumentException("bad-arg"), StatusCode.InvalidArgument).SetName("Argument_to_InvalidArgument");
        yield return new TestCaseData(new OperationCanceledException(), StatusCode.Cancelled).SetName("OperationCanceled_to_Cancelled");
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("denied"), StatusCode.PermissionDenied).SetName("AuthorizationDenied_to_PermissionDenied");
        yield return new TestCaseData(new Exception("boom"), StatusCode.Internal).SetName("Unexpected_to_Internal");
    }

    [Test]
    public void Remediate_maps_a_live_key_cap_breach_to_resource_exhausted_with_trailers()
    {
        // Regression: a remediation rebuilds the tree into a fresh destination one
        // entry at a time, so it runs under the per-tree admission caps. The
        // exception derives from InvalidOperationException, so without a typed arm
        // placed ahead of it the breach was reported as FailedPrecondition - a
        // precondition the caller cannot satisfy - instead of a capacity outcome.
        var control = Substitute.For<ILatticeSchemaControl>();
        control.RemediateAsync(Tree, Arg.Any<LatticeValueTransform>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException(
                "Write to tree 'orders/remediated/op-1' rejected: live key count 1200 has reached the configured LatticeOptions.MaxLiveKeys cap of 1000.",
                "orders/remediated/op-1",
                LatticeQuotaExceededException.KeysDimension,
                current: 1200,
                limit: 1000));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.Remediate(
            new RemediateRequest { TreeId = Tree, Transform = LatticeValueTransform.Passthrough(), TargetPolicy = JsonPolicy() },
            Context(LatticeSchemaGrpcMethods.RemediateMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(
                ex.Trailers.GetValue(LatticeSchemaGrpcService.QuotaDimensionTrailer),
                Is.EqualTo(LatticeQuotaExceededException.KeysDimension),
                "the client must be able to branch on the breached dimension without parsing prose");
            Assert.That(
                ex.Trailers.GetValue(LatticeSchemaGrpcService.QuotaTreeTrailer),
                Is.EqualTo("orders/remediated/op-1"));
            Assert.That(ex.Trailers.GetValue(LatticeSchemaGrpcService.QuotaCurrentTrailer), Is.EqualTo("1200"));
            Assert.That(ex.Trailers.GetValue(LatticeSchemaGrpcService.QuotaLimitTrailer), Is.EqualTo("1000"));
        });
    }

    [Test]
    public void MigrateToTargetVersion_maps_a_byte_cap_breach_to_resource_exhausted()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.MigrateToTargetVersionAsync(Tree, Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeQuotaExceededException(
                "Write to tree 'orders/remediated/op-2' rejected: estimated footprint 4096 bytes has reached the configured LatticeOptions.MaxEstimatedBytes cap of 2048 bytes.",
                "orders/remediated/op-2",
                LatticeQuotaExceededException.BytesDimension,
                current: 4096,
                limit: 2048));
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.MigrateToTargetVersion(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.MigrateToTargetVersionMethodName)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(
                ex.Trailers.GetValue(LatticeSchemaGrpcService.QuotaDimensionTrailer),
                Is.EqualTo(LatticeQuotaExceededException.BytesDimension));
            Assert.That(
                ex.Trailers.Select(static entry => entry.Key),
                Has.None.Contains("tenant"),
                "a quota trailer never carries a server-side tenant attribution");
        });
    }

    [Test]
    public void Unary_call_rethrows_an_rpc_exception_unchanged()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var original = new RpcException(new Status(StatusCode.ResourceExhausted, "quota"));
        control.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).ThrowsAsync(original);
        var service = CreateService(control);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetPolicy(
            new SchemaTreeRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.GetPolicyMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public async Task StreamDeadLetters_writes_every_entry_to_the_response_stream()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>()).Returns(DeadLetters(
            new LatticeSchemaDeadLetterEntry("k1", Array.Empty<byte>(), 0, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch),
            new LatticeSchemaDeadLetterEntry("k2", Array.Empty<byte>(), 0, "bad", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UnixEpoch)));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName));

        Assert.That(writer.Written.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
    }

    [Test]
    public void StreamDeadLetters_with_null_arguments_throws()
    {
        var service = CreateService(Substitute.For<ILatticeSchemaControl>());
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();
        var context = Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await service.StreamDeadLetters(null!, writer, context), Throws.ArgumentNullException);
            Assert.That(async () => await service.StreamDeadLetters(new SchemaTreeRequest { TreeId = Tree }, null!, context), Throws.ArgumentNullException);
            Assert.That(async () => await service.StreamDeadLetters(new SchemaTreeRequest { TreeId = Tree }, writer, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void StreamDeadLetters_translates_authorization_denied_to_permission_denied()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(FaultingDeadLetters(new LatticeAuthorizationDeniedException("denied")));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void StreamDeadLetters_translates_argument_error_to_invalid_argument()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(FaultingDeadLetters(new ArgumentException("bad")));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void StreamDeadLetters_translates_unexpected_error_to_internal()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(FaultingDeadLetters(new Exception("boom")));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
    }

    [Test]
    public void StreamDeadLetters_rethrows_an_rpc_exception_unchanged()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        var original = new RpcException(new Status(StatusCode.ResourceExhausted, "quota"));
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>()).Returns(FaultingDeadLetters(original));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName)));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public async Task StreamDeadLetters_swallows_cancellation_and_ends_cleanly()
    {
        var control = Substitute.For<ILatticeSchemaControl>();
        control.ListDeadLettersAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(FaultingDeadLetters(new OperationCanceledException()));
        var service = CreateService(control);
        var writer = new CollectingServerStreamWriter<LatticeSchemaDeadLetterEntry>();

        await service.StreamDeadLetters(
            new SchemaTreeRequest { TreeId = Tree },
            writer,
            Context(LatticeSchemaGrpcMethods.StreamDeadLettersMethodName));

        Assert.That(writer.Written, Is.Empty);
    }

#pragma warning disable CS1998 // async iterator intentionally yields synchronously
    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> DeadLetters(
        params LatticeSchemaDeadLetterEntry[] entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
        }
    }

    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> FaultingDeadLetters(Exception toThrow)
    {
        throw toThrow;
#pragma warning disable CS0162 // unreachable yield keeps this an async iterator
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998
}
