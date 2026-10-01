using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using LatticeOperationState = Orleans.Lattice.Api.Operations.LatticeOperationState;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="ILatticeTreeAdminOperations"/> on <see cref="LatticeTreeAdmin"/>
/// (#4124): each start verb authorizes as its blocking twin, starts a tracked
/// operation on a real <see cref="LatticeOperationRunner"/> over in-memory tracking
/// grains, and drives the engine's tracked grain call (or, for orphaned leaves, the
/// bounded batches) with the operation's ticket; status, list and cancel are scoped
/// fail-closed; and the deprecated blocking verbs wrap an operation.
/// </summary>
[TestFixture]
public sealed partial class LatticeTreeAdminOperationsTests
{
    private const string Tenant = "default";
    private const string ViewName = "orders-by-region";
    private const string SourceTree = "orders";
    private const string IndexName = "by-tag";
    private static readonly string IndexTree = LatticeConstants.TagIndexTreePrefix + IndexName;

    private Dictionary<string, FakeOperationGrain> _operations = null!;
    private ILatticeOperationIndexGrain _index = null!;
    private IGrainFactory _factory = null!;
    private LatticeOperationRunner _runner = null!;

    [SetUp]
    public void SetUp()
    {
        _operations = new Dictionary<string, FakeOperationGrain>(StringComparer.Ordinal);
        _index = Substitute.For<ILatticeOperationIndexGrain>();
        _factory = Substitute.For<IGrainFactory>();
        _factory.GetGrain<ILatticeOperationGrain>(Arg.Any<string>(), null).Returns(call => Operation(call.ArgAt<string>(0)));
        _factory.GetGrain<ILatticeOperationIndexGrain>(Arg.Any<string>(), null).Returns(_index);

        var silo = Substitute.For<ILocalSiloDetails>();
        silo.SiloAddress.Returns(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 22222), 1));
        _runner = new LatticeOperationRunner(
            _factory,
            silo,
            Options.Create(new LatticeOperationOptions()),
            NullLogger<LatticeOperationRunner>.Instance);
    }

    private FakeOperationGrain Operation(string key)
    {
        lock (_operations)
        {
            if (!_operations.TryGetValue(key, out var grain))
            {
                grain = new FakeOperationGrain(key);
                _operations[key] = grain;
            }

            return grain;
        }
    }

    private FakeOperationGrain OperationFor(string operationId) => Operation(LatticeOperationKey.For(Tenant, operationId));

    /// <summary>An access gate that allows exactly the listed operations.</summary>
    private sealed class ScopedGate(params LatticeOperation[] allowed) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(allowed.Contains(request.Operation)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("denied by test"));
    }

    private static readonly LatticeOperation[] AllGrants =
        [LatticeOperation.Read, LatticeOperation.Admin, LatticeOperation.TreeLifecycle];

    private LatticeTreeAdmin Create(LatticeOperation[]? grants = null, bool withRunner = true)
    {
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(new ViewRegistration(ViewName, SourceTree, Substitute.For<ILatticeViewProjection>()));

        return new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            _factory,
            new TreeAdminAccessAuthorizer(new ScopedGate(grants ?? AllGrants)),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            viewCatalog: catalog,
            viewFactory: Substitute.For<ILatticeViewFactory>(),
            tagIndexFactory: Substitute.For<ILatticeTagIndexFactory>(),
            operationRunner: withRunner ? _runner : null);
    }

    private IViewMaintainerGrain WireMaintainer()
    {
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns("view-" + ViewName);
        _factory.GetGrain<IViewMaintainerGrain>(ViewName, null).Returns(maintainer);
        return maintainer;
    }

    private async Task<FakeOperationGrain> UntilCompletedAsync(string operationId)
    {
        var grain = OperationFor(operationId);
        await TestPoll.UntilAsync(() => grain.Completion is not null, $"operation {operationId} to complete");
        return grain;
    }

    [Test]
    public async Task StartViewRebuildAsync_runs_the_tracked_rebuild_under_the_operation_ticket()
    {
        var maintainer = WireMaintainer();
        var facade = Create();

        var handle = await facade.StartViewRebuildAsync(ViewName, "rebuild-1");
        var grain = await UntilCompletedAsync("rebuild-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(TreeAdminOperationKinds.ViewRebuild));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { SourceTree }));
            Assert.That(grain.BeginRequest!.Phases, Is.EqualTo(new[]
            {
                TreeAdminOperationPhases.Scanning, TreeAdminOperationPhases.Projecting, TreeAdminOperationPhases.Swapping,
            }));
            Assert.That(grain.Completion!.State, Is.EqualTo(Orleans.Lattice.Operations.LatticeOperationState.Succeeded));
            Assert.That(grain.Completion.ResultReference, Is.EqualTo(ViewName));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.SourceTreeId], Is.EqualTo(SourceTree));
            Assert.That(grain.Completion.Result.ContainsKey(TreeAdminOperationResultKeys.DriftRepaired), Is.False);
        });
        await maintainer.Received(1).RebuildTrackedAsync(
            Arg.Is<LatticeOperationTicket>(t => t.OperationKey == LatticeOperationKey.For(Tenant, "rebuild-1")),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StartViewReconcileAsync_records_whether_drift_was_repaired()
    {
        var maintainer = WireMaintainer();
        maintainer.ReconcileTrackedAsync(Arg.Any<LatticeOperationTicket>(), Arg.Any<CancellationToken>()).Returns(true);
        var facade = Create();

        await facade.StartViewReconcileAsync(ViewName, "reconcile-1");
        var grain = await UntilCompletedAsync("reconcile-1");

        Assert.Multiple(() =>
        {
            Assert.That(grain.BeginRequest!.Kind, Is.EqualTo(TreeAdminOperationKinds.ViewReconcile));
            Assert.That(grain.Completion!.Result[TreeAdminOperationResultKeys.DriftRepaired], Is.EqualTo("true"));
        });
    }

    [Test]
    public void StartViewRebuildAsync_without_the_admin_grant_starts_nothing()
    {
        var maintainer = WireMaintainer();
        var facade = Create([LatticeOperation.Read]);

        Assert.That(async () => await facade.StartViewRebuildAsync(ViewName, "denied"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(OperationFor("denied").BeginRequest, Is.Null);
        maintainer.DidNotReceiveWithAnyArgs().RebuildTrackedAsync(default!, default);
    }

    [Test]
    public async Task StartTagIndexReconcileAsync_runs_the_tracked_sweep_and_records_its_counts()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(IndexTree).Returns(new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry());
        _factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var coordinator = Substitute.For<ITagIndexReconcileGrain>();
        coordinator.RunTrackedSweepAsync(Arg.Any<LatticeOperationTicket>(), Arg.Any<CancellationToken>())
            .Returns(new TagReconcileReport(3, 40, 12, 2));
        _factory.GetGrain<ITagIndexReconcileGrain>(IndexName, null).Returns(coordinator);
        var facade = Create();

        var handle = await facade.StartTagIndexReconcileAsync(IndexName, "sweep-1");
        var grain = await UntilCompletedAsync("sweep-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { IndexTree }));
            Assert.That(grain.Completion!.Result[TreeAdminOperationResultKeys.TreesCovered], Is.EqualTo("3"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.OrphanRowsRemoved], Is.EqualTo("2"));
        });
        await coordinator.Received(1).RunTrackedSweepAsync(
            Arg.Is<LatticeOperationTicket>(t => t.OperationKey == LatticeOperationKey.For(Tenant, "sweep-1")),
            Arg.Any<CancellationToken>());
        await coordinator.DidNotReceive().RunSweepAsync();
    }

    [Test]
    public async Task StartWalMoveAsync_runs_the_tracked_move_and_records_the_receipt()
    {
        var admin = Substitute.For<ILatticeAdminTrackedGrain>();
        admin.ExecuteWalMoveTrackedAsync(SourceTree, 1, "secondary", Arg.Any<WalMoveOptions?>(), Arg.Any<LatticeOperationTicket>(), Arg.Any<CancellationToken>())
            .Returns(new WalMoveReceipt
            {
                TreeId = SourceTree,
                Partition = 1,
                FromProviderKey = "primary",
                ToProviderKey = "secondary",
                PreviousPlacementVersion = 4,
                NewPlacementVersion = 5,
                CopiedFromOffset = 10,
                CopiedThroughOffset = 99,
                Outcome = WalMoveOutcome.Moved,
            });
        _factory.GetGrain<ILatticeAdminTrackedGrain>(LatticeConstants.AdminGrainKey, null).Returns(admin);
        var facade = Create();

        await facade.StartWalMoveAsync(SourceTree, 1, "secondary", operationId: "move-1");
        var grain = await UntilCompletedAsync("move-1");

        Assert.Multiple(() =>
        {
            Assert.That(grain.BeginRequest!.Kind, Is.EqualTo(TreeAdminOperationKinds.WalMove));
            Assert.That(grain.Completion!.ResultReference, Is.EqualTo(SourceTree));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.Outcome], Is.EqualTo(nameof(TreeWalMoveOutcome.Moved)));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.CopiedThroughOffset], Is.EqualTo("99"));
            Assert.That(grain.Completion.Result[TreeAdminOperationResultKeys.NewPlacementVersion], Is.EqualTo("5"));
        });
    }

    [Test]
    public void StartWalMoveAsync_requires_the_lifecycle_grant_and_rejects_a_reserved_tree()
    {
        var admin = Substitute.For<ILatticeAdminTrackedGrain>();
        _factory.GetGrain<ILatticeAdminTrackedGrain>(LatticeConstants.AdminGrainKey, null).Returns(admin);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await Create([LatticeOperation.Read, LatticeOperation.Admin]).StartWalMoveAsync(SourceTree, 0, "secondary"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await Create().StartWalMoveAsync(LatticeConstants.SystemTreePrefix + "x", 0, "secondary"),
                Throws.ArgumentException);
            Assert.That(async () => await Create().StartWalMoveAsync(SourceTree, 0, ""), Throws.ArgumentException);
        });
        admin.DidNotReceiveWithAnyArgs().ExecuteWalMoveTrackedAsync(default!, default, default!, default, default!, default);
    }

    [TestCase("has space")]
    [TestCase("slash/id")]
    public void A_malformed_operation_id_is_refused(string operationId)
    {
        WireMaintainer();
        Assert.That(async () => await Create().StartViewRebuildAsync(ViewName, operationId), Throws.ArgumentException);
    }

    [Test]
    public void Start_verbs_without_the_coordinator_throw_invalid_operation()
    {
        WireMaintainer();
        var facade = Create(withRunner: false);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.StartViewRebuildAsync(ViewName), Throws.InvalidOperationException);
            Assert.That(async () => await facade.StartOrphanedLeavesAuditAsync(SourceTree), Throws.InvalidOperationException);
            Assert.That(async () => await facade.GetOperationStatusAsync("any"), Throws.InvalidOperationException);
        });
    }
}
