using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Hosting;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using LatticeOperationState = Orleans.Lattice.Api.Operations.LatticeOperationState;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStorageUsageOperations"/> (#4126), the
/// accept-then-poll fresh storage usage, driven on a real
/// <see cref="LatticeOperationRunner"/> over in-memory operation grains: a start
/// needs cluster telemetry and returns at once, the refresh re-measures every tree
/// with its cache bypassed and reports one unit per tree, a tree that cannot be
/// measured is a flagged partial reading rather than a failure, and status, listing
/// and cancel are scoped fail-closed to the refresh kind and telemetry holders.
/// </summary>
[TestFixture]
public sealed class LatticeStorageUsageOperationsTests
{
    private const string Tenant = "default";

    private sealed class FixedGate(bool allow) : ILatticeAccessGate
    {
        public LatticeAccessRequest Last { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Last = request;
            return new(allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied"));
        }
    }

    private sealed class Harness
    {
        public required InMemoryOperationRunner Operations { get; init; }

        public required IGrainFactory Factory { get; init; }

        public required Dictionary<string, ILatticeStorageUsage> Trees { get; init; }

        public required LatticeStorageUsageOperations Facade { get; init; }

        public LatticeStorageUsageOperations With(ILatticeAccessGate gate) => Build(Operations, Factory, gate);
    }

    private static LatticeStorageUsageOperations Build(InMemoryOperationRunner operations, IGrainFactory factory, ILatticeAccessGate gate)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new(
            operations.Runner,
            factory,
            new TreeAdminAccessAuthorizer(gate),
            new NullTenantContextResolver(),
            options,
            NullLogger<LatticeStorageUsageOperations>.Instance);
    }

    private static Harness Create(ILatticeAccessGate? gate = null, params string[] treeIds)
    {
        var operations = new InMemoryOperationRunner();
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns((IReadOnlyList<string>)treeIds);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var trees = new Dictionary<string, ILatticeStorageUsage>(StringComparer.Ordinal);
        foreach (var treeId in treeIds)
        {
            var usage = Substitute.For<ILatticeStorageUsage>();
            usage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(new TreeStorageUsageReport
            {
                TreeId = treeId,
                WalRetainedBytes = 10,
                SnapshotBytes = 20,
                LeafStateBytes = 30,
                TotalBytes = 60,
                SampledAt = DateTimeOffset.UtcNow,
            });
            factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(usage);
            trees[treeId] = usage;
        }

        return new Harness
        {
            Operations = operations,
            Factory = factory,
            Trees = trees,
            Facade = Build(operations, factory, gate ?? new FixedGate(true)),
        };
    }

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_refresh_re_measures_every_tree_and_records_the_cluster_totals()
    {
        var h = Create(null, "a", "b", "c");

        var handle = await h.Facade.StartStorageUsageRefreshAsync("refresh-1");
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(StorageUsageRefreshOperation.Kind));
            Assert.That(handle.Created, Is.True);
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.Phase, Is.EqualTo(StorageUsageRefreshOperation.MeasuringPhase));
            Assert.That(status.CompletedUnits, Is.EqualTo(3));
            Assert.That(status.TotalUnits, Is.EqualTo(3));
            Assert.That(status.UnitName, Is.EqualTo(StorageUsageRefreshOperation.TreesUnit));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(status.Result, out var summary), Is.True);
            Assert.That(summary!.TreeCount, Is.EqualTo(3));
            Assert.That(summary.TotalBytes, Is.EqualTo(180));
            Assert.That(summary.Partial, Is.False);
            Assert.That(summary.Deep, Is.True);
        });

        foreach (var tree in h.Trees.Values)
        {
            await tree.Received(1).GetReportAsync(true, Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public async Task Progress_counts_one_tree_per_unit_against_the_tree_count()
    {
        var h = Create(null, "a", "b");

        var handle = await h.Facade.StartStorageUsageRefreshAsync("refresh-progress");
        await UntilTerminalAsync(h.Facade, handle.OperationId);
        var reports = h.Operations.Reports(Tenant, "refresh-progress")
            .Where(r => r.Phase == StorageUsageRefreshOperation.MeasuringPhase)
            .Skip(1) // The runner opens the first declared phase before the work reports.
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(reports[0].CompletedUnits, Is.Zero);
            Assert.That(reports.Select(r => r.TotalUnits), Is.All.EqualTo(2));
            Assert.That(reports[^1].CompletedUnits, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_tree_that_cannot_be_measured_is_a_partial_reading_not_a_failed_refresh()
    {
        var h = Create(null, "a", "b");
        h.Trees["b"].GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException("slow"));

        var handle = await h.Facade.StartStorageUsageRefreshAsync();
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(status.Result, out var summary), Is.True);
            Assert.That(summary!.Partial, Is.True);
            Assert.That(summary.TotalBytes, Is.EqualTo(60));
        });
    }

    [Test]
    public async Task A_start_needs_cluster_telemetry_and_a_denied_caller_starts_nothing()
    {
        var gate = new FixedGate(true);
        var h = Create(gate, "a");

        await h.Facade.StartStorageUsageRefreshAsync("refresh-gated");
        var denied = h.With(new FixedGate(false));

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Telemetry));
            Assert.That(
                async () => await denied.StartStorageUsageRefreshAsync("refresh-denied"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(h.Operations.Record(Tenant, "refresh-denied"), Is.Null);
        });
    }

    [Test]
    public async Task A_caller_without_telemetry_cannot_see_list_or_cancel_a_refresh()
    {
        var h = Create(null, "a");
        var handle = await h.Facade.StartStorageUsageRefreshAsync("refresh-hidden");
        await UntilTerminalAsync(h.Facade, handle.OperationId);
        var stranger = h.With(new FixedGate(false));

        Assert.Multiple(async () =>
        {
            Assert.That(await stranger.GetOperationStatusAsync("refresh-hidden"), Is.Null);
            Assert.That(await stranger.CancelOperationAsync("refresh-hidden"), Is.Null);
            Assert.That((await stranger.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Is.Empty);
            Assert.That((await h.Facade.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Another_kind_of_operation_is_neither_read_nor_listed()
    {
        var h = Create(null, "a");
        var other = await h.Operations.Runner.StartAsync(
            new LatticeOperationStart { TenantId = Tenant, OperationId = "view-1", Kind = "treeadmin.view-rebuild" },
            static (_, _) => Task.FromResult(0),
            static _ => LatticeOperationCompletion.Succeeded());
        await other.Completion!;

        Assert.Multiple(async () =>
        {
            Assert.That(await h.Facade.GetOperationStatusAsync("view-1"), Is.Null);
            Assert.That(await h.Facade.CancelOperationAsync("view-1"), Is.Null);
            Assert.That((await h.Facade.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Is.Empty);
        });
    }

    [Test]
    public async Task Cancelling_a_running_refresh_stops_it_and_reads_as_cancelled()
    {
        var h = Create(null, "a");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Trees["a"].GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(async call =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, call.ArgAt<CancellationToken>(1));
            return new TreeStorageUsageReport { TreeId = "a" };
        });

        var handle = await h.Facade.StartStorageUsageRefreshAsync("refresh-cancel");
        await started.Task;
        await h.Facade.CancelOperationAsync(handle.OperationId);
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.That(status.State, Is.EqualTo(LatticeOperationState.Cancelled));
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_starts_nothing()
    {
        var h = Create(null, "a");

        var first = await h.Facade.StartStorageUsageRefreshAsync("refresh-twice");
        await UntilTerminalAsync(h.Facade, first.OperationId);
        var second = await h.Facade.StartStorageUsageRefreshAsync("refresh-twice");

        Assert.That(second.Created, Is.False);
        await h.Trees["a"].Received(1).GetReportAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Malformed_or_missing_arguments_are_refused()
    {
        var h = Create(null, "a");

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Facade.StartStorageUsageRefreshAsync("has space"), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.GetOperationStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.CancelOperationAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.ListOperationsAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var runner = new InMemoryOperationRunner().Runner;
        var factory = Substitute.For<IGrainFactory>();
        var authorizer = new TreeAdminAccessAuthorizer(new FixedGate(true));
        var tenants = new NullTenantContextResolver();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var logger = NullLogger<LatticeStorageUsageOperations>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeStorageUsageOperations(null!, factory, authorizer, tenants, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeStorageUsageOperations(runner, null!, authorizer, tenants, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeStorageUsageOperations(runner, factory, null!, tenants, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeStorageUsageOperations(runner, factory, authorizer, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeStorageUsageOperations(runner, factory, authorizer, tenants, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new LatticeStorageUsageOperations(runner, factory, authorizer, tenants, options, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddLatticeTreeAdminApi_registers_the_storage_usage_operations_once()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        builder.AddLatticeTreeAdminApi();
        builder.AddLatticeTreeAdminApi();

        var registrations = services.Where(d => d.ServiceType == typeof(ILatticeStorageUsageOperations)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(registrations, Has.Count.EqualTo(1));
            Assert.That(registrations[0].ImplementationType, Is.EqualTo(typeof(LatticeStorageUsageOperations)));
        });
    }

    [Test]
    public void The_refresh_result_map_round_trips_and_rejects_a_foreign_map()
    {
        var summary = new ClusterStorageUsageSummary
        {
            TreeCount = 2,
            WalRetainedBytes = 1,
            SnapshotBytes = -1,
            LeafStateBytes = 3,
            TotalBytes = 4,
            Partial = true,
            SampledAt = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero),
        };

        var map = StorageUsageRefreshResults.ToResultMap(summary);

        Assert.Multiple(() =>
        {
            Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out var read), Is.True);
            Assert.That(read, Is.EqualTo(summary with { Deep = true }));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(new Dictionary<string, string>(), out var none), Is.False);
            Assert.That(none, Is.Null);
            Assert.That(() => StorageUsageRefreshResults.ToResultMap(null!), Throws.ArgumentNullException);
            Assert.That(() => StorageUsageRefreshResults.TryReadSummary(null!, out _), Throws.ArgumentNullException);
        });
    }
}
