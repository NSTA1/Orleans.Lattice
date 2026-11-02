using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Regression coverage for issue #5419: a core access-gate denial
/// (<see cref="LatticeAuthorizationDeniedException"/>) reaching any of the
/// state binding's four fault ladders - the shared unary path, the metrics poll,
/// and both subscriptions - leaves as <see cref="StatusCode.PermissionDenied"/>
/// and is not logged as a server fault. Before the fix it fell through to the
/// generic arm, left as <see cref="StatusCode.Internal"/>, and a client read the
/// refusal of one whole-tree metrics sample under a narrowed grant as the
/// endpoint failing.
/// </summary>
[TestFixture]
public sealed class LatticeStateGrpcServiceGateDenialTests
{
    private ServiceProvider _services = null!;
    private LatticeStateGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeStateGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    // The shape the gate throws for a prefix-scoped grant asking for a whole-tree aggregate.
    private static LatticeAuthorizationDeniedException NarrowedGrantDenial() => new(
        "export-ledger",
        LatticeOperation.Read,
        "dave",
        "Range read over tree 'export-ledger' [(start), (end)) filtered per-key by policy.");

    private LatticeStateGrpcService CreateService(
        RecordingLogger logger,
        ILatticeStateQuery? query = null,
        ILatticeStateObserver? observer = null,
        ILatticeStateMetricsObserver? metricsObserver = null) =>
        new(
            _methods,
            query ?? Substitute.For<ILatticeStateQuery>(),
            observer ?? Substitute.For<ILatticeStateObserver>(),
            metricsObserver ?? Substitute.For<ILatticeStateMetricsObserver>(),
            Substitute.For<ILatticeStateApiCredentialBridge>(),
            Substitute.For<ILatticeStateApiAuthSchemeSource>(),
            Options.Create(new LatticeStateApiGrpcOptions()),
            logger);

    private static async IAsyncEnumerable<T> Throwing<T>(Exception thrown)
    {
        await Task.Yield();
        throw thrown;
#pragma warning disable CS0162 // unreachable - the compiler needs a yield to make this an iterator
        yield break;
#pragma warning restore CS0162
    }

    private static void AssertPermissionDenied(RpcException ex, RecordingLogger logger)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("dave").And.Contain("export-ledger"));
            Assert.That(logger.ErrorCount, Is.Zero, "a refusal is not a server fault and is not logged as one");
        });
    }

    [Test]
    public void GetMetricsSnapshot_maps_a_gate_denial_to_PermissionDenied()
    {
        var logger = new RecordingLogger();
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(NarrowedGrantDenial());
        var service = CreateService(logger, metricsObserver: metricsObserver);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetMetricsSnapshot(
            new TreeMetricsRequest { TreeIds = ["export-ledger"] },
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName)))!;

        AssertPermissionDenied(ex, logger);
    }

    [Test]
    public void ObserveMetrics_maps_a_gate_denial_to_PermissionDenied()
    {
        var logger = new RecordingLogger();
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.ObserveAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Throwing<TreeMetricsSnapshot>(NarrowedGrantDenial()));
        var service = CreateService(logger, metricsObserver: metricsObserver);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ObserveMetrics(
            new TreeMetricsRequest { TreeIds = ["export-ledger"] },
            new RecordingServerStreamWriter<TreeMetricsSnapshot>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveMetricsMethodName)))!;

        AssertPermissionDenied(ex, logger);
    }

    [Test]
    public void ObserveChanges_maps_a_gate_denial_to_PermissionDenied()
    {
        var logger = new RecordingLogger();
        var observer = Substitute.For<ILatticeStateObserver>();
        observer.ObserveAsync(Arg.Any<StateObserveRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Throwing<StateChangeNotification>(NarrowedGrantDenial()));
        var service = CreateService(logger, observer: observer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.ObserveChanges(
            new StateObserveRequest { TreeId = "export-ledger" },
            new RecordingServerStreamWriter<StateChangeNotification>(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.ObserveChangesMethodName)))!;

        AssertPermissionDenied(ex, logger);
    }

    [Test]
    public void A_unary_query_maps_a_gate_denial_to_PermissionDenied()
    {
        var logger = new RecordingLogger();
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetDeadLetterCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(NarrowedGrantDenial());
        var service = CreateService(logger, query: query);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetDeadLetterCount(
            new DeadLetterCountRequest { TreeId = "export-ledger" },
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetDeadLetterCountMethodName)))!;

        AssertPermissionDenied(ex, logger);
    }

    [Test]
    public void An_unexpected_fault_is_still_logged_and_left_as_Internal()
    {
        // The control: the new arm must catch only the gate's denial, not widen
        // into the generic fault handling every other failure still relies on.
        var logger = new RecordingLogger();
        var metricsObserver = Substitute.For<ILatticeStateMetricsObserver>();
        metricsObserver.SampleAsync(Arg.Any<TreeMetricsRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("boom"));
        var service = CreateService(logger, metricsObserver: metricsObserver);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.GetMetricsSnapshot(
            new TreeMetricsRequest(),
            StateGrpcCallContext.ForMethod(LatticeStateGrpcMethods.GetMetricsSnapshotMethodName)))!;

        Assert.Multiple(() =>
        {
            Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(logger.ErrorCount, Is.EqualTo(1));
        });
    }

    private sealed class RecordingLogger : ILogger<LatticeStateGrpcService>
    {
        public int ErrorCount { get; private set; }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Error)
            {
                ErrorCount++;
            }
        }
    }
}
