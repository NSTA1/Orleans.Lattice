using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Ordering coverage for the restore path's fail-closed gate. It is not enough
/// that <see cref="ILatticeBackupRestoreService.RestoreAsync"/> and
/// <see cref="ILatticeBackupRestoreService.RestoreSetAsync"/> deny an
/// unauthorized caller - the denial has to happen <b>before</b> the
/// coordinated-restore saga dispatcher is consulted.
/// <para>
/// The dispatcher returns the saga's own result for a replicated target and the
/// restore service returns it directly, so a gate placed after dispatch is
/// unreachable on exactly the path that fans work out across the fleet: probing
/// admission over the catalog, deciding capacity, fencing every peer cluster and
/// running the cross-cluster coordinator, all before any participant reaches its
/// own check. These fixtures therefore assert the spy was never invoked, which is
/// the load-bearing half; asserting only the exception type would pass against
/// the defective ordering too.
/// </para>
/// </summary>
[Category("Integration")]
public sealed class RestoreAuthorizationOrderingTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task RestoreAsync_denies_before_consulting_the_coordinated_saga_dispatcher()
    {
        await _fixture.InitializeAsync();
        const string tree = "ordering-orders";
        var source = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await source.SetAsync("k1", Bytes("v1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("ordering", BackupScopeSelector.WholeTree(tree)));

        var spy = new RecordingRestoreSagaDispatcher();
        var denying = _fixture.CreateRestoreServiceWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null),
            spy);

        Assert.That(
            async () => await denying.RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, "ordering-target")),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        Assert.That(spy.DispatchCount, Is.Zero,
            "authorization must precede coordinated-restore dispatch, not follow it");
    }

    [Test]
    public async Task RestoreSetAsync_denies_before_consulting_the_coordinated_saga_dispatcher()
    {
        await _fixture.InitializeAsync();
        const string treeA = "ordering-set-a";
        const string treeB = "ordering-set-b";
        await _fixture.GrainFactory.GetGrain<ILattice>(treeA).SetAsync("k", Bytes("a"));
        await _fixture.GrainFactory.GetGrain<ILattice>(treeB).SetAsync("k", Bytes("b"));

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "ordering-set",
            [BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB)]));

        var setId = set.SetManifest.SetId!;
        Assert.That(setId, Is.Not.Null,
            "a set spanning more than one tree stamps its members, so it carries a resolvable id");
        await AwaitSetMembersAsync(setId, 2);

        var spy = new RecordingRestoreSagaDispatcher();
        var denying = _fixture.CreateRestoreServiceWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null),
            spy);

        Assert.That(
            async () => await denying.RestoreSetAsync(setId),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        Assert.That(spy.SetDispatchCount, Is.Zero,
            "a set restore must authorize every member target before dispatching the saga");
    }

    [Test]
    public async Task ProbeAdmissionAsync_fails_closed_under_a_denying_gate()
    {
        await _fixture.InitializeAsync();
        const string tree = "ordering-probe";
        var source = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        await source.SetAsync("k1", Bytes("v1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("probe", BackupScopeSelector.WholeTree(tree)));

        var denying = (ILatticeCoordinatedRestoreEngine)_fixture.CreateRestoreServiceWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null));

        // ProbeAdmissionAsync was the one coordinated-restore engine seam with no
        // gate, and it answers first: its report names the tree a backup id belongs
        // to together with its materialisation cost and shard count, which the saga
        // dispatcher surfaces to the caller. Ungated it is a metadata-enumeration
        // oracle over the whole catalog.
        Assert.That(
            async () => await denying.ProbeAdmissionAsync(
                new LatticeRestoreRequest(backup.BackupId, tree)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    private async Task AwaitSetMembersAsync(string setId, int expected)
    {
        var resolver = _fixture.SiloServices.GetRequiredService<ILatticeBackupSetResolver>();
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < 10_000)
        {
            var members = await resolver.ResolveMembersAsync(setId);
            if (members.Count >= expected)
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"The catalog never listed {expected} member(s) for set '{setId}'.");
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class RecordingRestoreSagaDispatcher : IRestoreSagaDispatcher
    {
        public int DispatchCount { get; private set; }

        public int SetDispatchCount { get; private set; }

        public Task<LatticeRestoreResult?> TryDispatchAsync(
            LatticeRestoreRequest request,
            CancellationToken cancellationToken = default)
        {
            DispatchCount++;
            return Task.FromResult<LatticeRestoreResult?>(null);
        }

        public Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync(
            string setId,
            LatticeRestoreMode mode,
            CancellationToken cancellationToken = default)
        {
            SetDispatchCount++;
            return Task.FromResult<IReadOnlyList<LatticeRestoreResult>?>(null);
        }
    }

    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
