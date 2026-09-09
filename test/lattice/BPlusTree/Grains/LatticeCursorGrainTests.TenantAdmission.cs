using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage of the per-tenant read charge on snapshot cursor pages.
/// </summary>
/// <remarks>
/// <para>
/// Live key / entry cursors page through the public <c>ILattice</c> scan surface,
/// so the data-plane grain charges them at its own access-gate seam. The snapshot
/// cursor does not: it reads snapshot leaf grains directly, which made it an
/// unmetered whole-keyspace read a tenant could open as often as it liked - the
/// same noisy-neighbour hole the read charge exists to close, reached through a
/// different verb.
/// </para>
/// <para>
/// These tests pin that the charge is taken per page, that it follows rather than
/// precedes the gate (the tenant billed is a caller assertion only the gate
/// validates), and that it is inert when tenancy is not registered.
/// </para>
/// </remarks>
public partial class LatticeCursorGrainTests
{
    /// <summary>
    /// Records every read charge and returns a fixed decision, so both "was it
    /// charged" and "was it charged before the gate ran" are observable.
    /// </summary>
    private sealed class RecordingReadAdmissionController(bool active, bool admitRead)
        : ITenantAdmissionController
    {
        public int ReadCallCount { get; private set; }

        public bool IsActive => active;

        public bool IsReadAdmitted(TenantId tenant, string treeId)
        {
            ReadCallCount++;
            return admitRead;
        }

        public ValueTask<bool> IsAdmittedAsync(
            TenantId tenant, string treeId, CancellationToken cancellationToken = default) =>
            new(true);
    }

    [TearDown]
    public void ClearActiveTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>
    /// The defect: an authorized snapshot page was never charged.
    /// </summary>
    [Test]
    public async Task NextKeysAsync_snapshot_charges_the_tenant_read_budget_per_page()
    {
        var token = Guid.NewGuid();
        var controller = new RecordingReadAdmissionController(active: true, admitRead: true);
        var (grain, _, factory) = CreateSnapshotGrain(s =>
        {
            s.AddSingleton<ILatticeAccessGate>(new SnapshotGate(_ => LatticeAccessDecision.Allow()));
            s.AddSingleton<ITenantAdmissionController>(controller);
        });
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var first = await grain.NextKeysAsync(2);
        Assert.That(first.Keys, Is.Not.Empty);
        Assert.That(controller.ReadCallCount, Is.EqualTo(1));

        // Per page, not per cursor: a cursor is unbounded in length, so charging
        // once at open would bound scan initiations rather than scan work.
        await grain.NextKeysAsync(2);
        Assert.That(controller.ReadCallCount, Is.EqualTo(2));
    }

    [Test]
    public async Task NextKeysAsync_snapshot_over_the_rate_budget_is_refused()
    {
        var token = Guid.NewGuid();
        var controller = new RecordingReadAdmissionController(active: true, admitRead: false);
        var (grain, _, factory) = CreateSnapshotGrain(s =>
        {
            s.AddSingleton<ILatticeAccessGate>(new SnapshotGate(_ => LatticeAccessDecision.Allow()));
            s.AddSingleton<ITenantAdmissionController>(controller);
        });
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => grain.NextKeysAsync(2));
    }

    /// <summary>
    /// A turn the gate never adjudicated must never be charged: the active tenant
    /// is a caller-asserted value that only the gate validates, so charging an
    /// unadjudicated turn would let a caller burn a named victim's budget.
    /// </summary>
    [Test]
    public async Task NextKeysAsync_snapshot_under_system_origin_is_not_charged()
    {
        var token = Guid.NewGuid();
        var controller = new RecordingReadAdmissionController(active: true, admitRead: false);
        var (grain, _, factory) = CreateSnapshotGrain(s =>
        {
            s.AddSingleton<ILatticeAccessGate>(new SnapshotGate(_ => LatticeAccessDecision.Allow()));
            s.AddSingleton<ITenantAdmissionController>(controller);
        });
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();

        var page = await grain.NextKeysAsync(2);
        Assert.That(page.Keys, Is.Not.Empty);
        Assert.That(controller.ReadCallCount, Is.Zero);
    }

    /// <summary>
    /// A null gate is a permissive <em>policy</em>, not an absent adjudication:
    /// the caller is still an ordinary tenant caller, so its page must still be
    /// metered. Folding the null gate into the skip predicate would turn an
    /// unrelated composition choice into a quota bypass.
    /// </summary>
    [Test]
    public async Task NextKeysAsync_snapshot_with_no_gate_is_still_charged()
    {
        var token = Guid.NewGuid();
        var controller = new RecordingReadAdmissionController(active: true, admitRead: true);
        var (grain, _, factory) = CreateSnapshotGrain(
            s => s.AddSingleton<ITenantAdmissionController>(controller));
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var page = await grain.NextKeysAsync(2);
        Assert.That(page.Keys, Is.Not.Empty);
        Assert.That(controller.ReadCallCount, Is.EqualTo(1));
    }

    /// <summary>
    /// Tenancy is an optional add-on: an unregistered or inactive controller must
    /// leave the snapshot page path untouched.
    /// </summary>
    [Test]
    public async Task NextKeysAsync_snapshot_with_an_inactive_controller_is_not_charged()
    {
        var token = Guid.NewGuid();
        var controller = new RecordingReadAdmissionController(active: false, admitRead: false);
        var (grain, _, factory) = CreateSnapshotGrain(s =>
        {
            s.AddSingleton<ILatticeAccessGate>(new SnapshotGate(_ => LatticeAccessDecision.Allow()));
            s.AddSingleton<ITenantAdmissionController>(controller);
        });
        WireSnapshotShard(factory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeTokenCoordinate(token, (0, 0)));

        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var page = await grain.NextKeysAsync(2);
        Assert.That(page.Keys, Is.Not.Empty);
        Assert.That(controller.ReadCallCount, Is.Zero);
    }
}
