using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for tenant scoping on the <see cref="ILatticeBackupControl"/> facade.
/// A tree name a caller supplies is an unqualified, tenant-local name, so the
/// facade composes it into the caller's effective <c>t/{tenant}/{name}</c> id
/// once at method entry and uses the composed scope for both the authorization
/// call and the operation. Two tenants that pick the same unqualified name
/// therefore reach two different trees, and a cluster with no tenancy add-on is
/// unchanged.
/// <para>
/// The complementary half - the tree ids the facade must <i>not</i> compose
/// (manifest-derived scopes and the platform-owned catalog tree) - lives in
/// <c>LatticeBackupControlTenancyTests.ManifestScopes.cs</c>.
/// </para>
/// </summary>
[Category("Integration")]
public sealed partial class LatticeBackupControlTenancyTests
{
    private const string LocalName = "orders";
    private const string LegacyName = "legacy-orders";
    private const string Acme = "acme";
    private const string Globex = "globex";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Caller-supplied scopes are composed ----------------------------

    [Test]
    public async Task CreateBackupAsync_composes_the_caller_supplied_scope_under_the_active_tenant()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "acme-secret");

        var control = ControlFor(Acme);
        var captured = await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        Assert.That(captured.Manifest.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
    }

    [Test]
    public async Task CreateBackupAsync_two_tenants_using_the_same_name_capture_different_trees()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "acme-secret");
        await SeedAsync(Effective(Globex, LocalName), "k", "globex-secret");

        var request = new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName));
        var acme = await ControlFor(Acme).CreateBackupAsync(request);
        var globex = await ControlFor(Globex).CreateBackupAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(acme.Manifest.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
            Assert.That(globex.Manifest.Scope.TreeId, Is.EqualTo(Effective(Globex, LocalName)));
            Assert.That(acme.Manifest.Scope.TreeId, Is.Not.EqualTo(globex.Manifest.Scope.TreeId));
        });
    }

    [Test]
    public async Task A_tenant_capture_and_restore_round_trip_stays_inside_the_tenants_namespace()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "acme-secret");

        var control = ControlFor(Acme);
        var captured = await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        // The caller names its restore target tenant-locally, exactly as it named
        // the capture source.
        var restored = await control.RestoreBackupAsync(
            new LatticeRestoreRequest(captured.BackupId, targetTreeId: "orders-copy"));

        Assert.That(restored.TargetTreeId, Is.EqualTo(Effective(Acme, "orders-copy")));
        Assert.That(await ReadAsync(Effective(Acme, "orders-copy"), "k"), Is.EqualTo("acme-secret"));

        // The untenanted name the caller typed was never materialised.
        Assert.That(await ReadAsync("orders-copy", "k"), Is.Null);
    }

    [Test]
    public async Task CreateIncrementalBackupAsync_composes_the_caller_supplied_scope()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v1");

        var control = ControlFor(Acme);
        var full = await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));
        await SeedAsync(Effective(Acme, LocalName), "k2", "v2");

        var incremental = await control.CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "incr", BackupScopeSelector.WholeTree(LocalName), full.BackupId));

        Assert.That(incremental.Manifest.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
    }

    [Test]
    public async Task CreateBackupSetAsync_composes_every_member_scope()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, "a"), "k", "va");
        await SeedAsync(Effective(Acme, "b"), "k", "vb");

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);

        await control.CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "set",
                [BackupScopeSelector.WholeTree("a"), BackupScopeSelector.WholeTree("b")]));

        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Backup),
            Is.EquivalentTo(new[] { Effective(Acme, "a"), Effective(Acme, "b") }));
    }

    [Test]
    public async Task ScheduleBackupAsync_composes_the_scope_so_the_schedule_is_per_tenant()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        await ControlFor(Acme).ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(
                BackupScopeSelector.WholeTree(LocalName), incremental: false, TimeSpan.FromHours(6)));

        var acmeStatus = await ControlFor(Acme).GetScopeStatusAsync(BackupScopeSelector.WholeTree(LocalName));
        var globexStatus = await ControlFor(Globex).GetScopeStatusAsync(BackupScopeSelector.WholeTree(LocalName));

        Assert.Multiple(() =>
        {
            Assert.That(acmeStatus, Is.Not.Null);
            Assert.That(acmeStatus!.FullScheduleRegistered, Is.True);
            Assert.That(acmeStatus.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));

            // The other tenant used the identical unqualified name and sees no
            // schedule: the two resolved to different scheduler grains.
            Assert.That(globexStatus, Is.Null);
        });
    }

    [Test]
    public async Task CancelScheduleAsync_composes_the_scope_and_reaches_the_same_schedule()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        var control = ControlFor(Acme);
        await control.ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(
                BackupScopeSelector.WholeTree(LocalName), incremental: false, TimeSpan.FromHours(6)));

        await control.CancelScheduleAsync(BackupScopeSelector.WholeTree(LocalName), incremental: false);

        var status = await control.GetScopeStatusAsync(BackupScopeSelector.WholeTree(LocalName));
        Assert.That(status?.FullScheduleRegistered ?? false, Is.False);
    }

    [Test]
    public async Task GetScopeStatusAsync_matches_chain_depth_against_the_effective_scope()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        var control = ControlFor(Acme);
        await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var status = await control.GetScopeStatusAsync(BackupScopeSelector.WholeTree(LocalName));

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.Not.Null);
            Assert.That(status!.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
            Assert.That(status.ChainDepth, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_probes_and_reports_the_effective_scope()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);

        var caps = await control.ProbeCapabilitiesAsync(
            BackupScopeSelector.Prefix(LocalName, "eu/"));

        Assert.Multiple(() =>
        {
            Assert.That(caps.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
            Assert.That(caps.Scope.KeyOrPrefix, Is.EqualTo("eu/"));
            Assert.That(gate.TreeIdsFor(LatticeOperation.Backup), Is.EqualTo(new[] { Effective(Acme, LocalName) }));
            Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Is.EqualTo(new[] { Effective(Acme, LocalName) }));
        });
    }

    [Test]
    public async Task RestoreBackupAsync_composes_a_caller_supplied_target_tree()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Globex, LocalName), "k", "v");

        var captured = await ControlFor(Globex).CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.RestoreBackupAsync(
            new LatticeRestoreRequest(captured.BackupId, targetTreeId: "landing"));

        // Only the caller's own target was composed; the manifest's captured tree
        // is not what was authorized.
        Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Does.Contain(Effective(Acme, "landing")));
    }

    [Test]
    public async Task ColdRestoreAsync_composes_a_caller_supplied_target_tree()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Globex, LocalName), "k", "v");

        var captured = await ControlFor(Globex).CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.ColdRestoreAsync(
            new LatticeRestoreRequest(captured.BackupId, targetTreeId: "cold-landing"));

        Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Does.Contain(Effective(Acme, "cold-landing")));
    }

    [Test]
    public async Task RestoreBackupAsync_composes_the_target_before_the_sub_region_scope_is_built()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "eu/k", "v");

        var captured = await ControlFor(Acme).CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.RestoreBackupAsync(
            new LatticeRestoreRequest(
                captured.BackupId,
                targetTreeId: "landing",
                scope: BackupScopeSelector.Prefix(LocalName, "eu/")));

        var restoreRequests = gate.RequestsFor(LatticeOperation.Restore);
        Assert.Multiple(() =>
        {
            Assert.That(restoreRequests, Is.Not.Empty);
            Assert.That(restoreRequests[0].TreeId, Is.EqualTo(Effective(Acme, "landing")));
            Assert.That(restoreRequests[0].Key, Is.EqualTo("eu/"));
        });
    }

    [Test]
    public async Task RevertRestoreAsync_composes_a_caller_supplied_target_and_the_engine_sees_the_same_tree()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        var control = ControlFor(Acme);
        var captured = await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var restored = await control.RestoreBackupAsync(
            new LatticeRestoreRequest(
                captured.BackupId, targetTreeId: LocalName, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(restored.TargetTreeId, Is.EqualTo(Effective(Acme, LocalName)));

        // The caller hands the result back naming its own tenant-local tree. The
        // facade composes it AND writes it back onto the result, so the restore
        // engine - which re-authorizes and then acts on that same field - reverts
        // exactly the tree the facade authorized.
        var gate = new RecordingAccessGate();
        var recording = ControlFor(Acme, gate);
        await recording.RevertRestoreAsync(restored with { TargetTreeId = LocalName });

        Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Is.Not.Empty);
        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Restore),
            Is.All.EqualTo(Effective(Acme, LocalName)));
    }

    [Test]
    public async Task ListBackupsAsync_composes_the_caller_supplied_tree_id_filter()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        var control = ControlFor(Acme);
        await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        var mine = await control.ListBackupsAsync(
            new BackupCatalogRequest { OrderByCreatedDescending = true, TreeId = LocalName });

        // The identical unqualified filter under another tenant resolves to that
        // tenant's namespace and matches nothing.
        var theirs = await ControlFor(Globex).ListBackupsAsync(
            new BackupCatalogRequest { OrderByCreatedDescending = true, TreeId = LocalName });

        Assert.Multiple(() =>
        {
            Assert.That(mine.Entries, Has.Count.EqualTo(1));
            Assert.That(mine.Entries[0].Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
            Assert.That(theirs.Entries, Is.Empty);
        });
    }

    // ---- Tenancy off: byte-for-byte unchanged ---------------------------

    [Test]
    public async Task With_no_tenancy_add_on_the_caller_scope_reaches_the_gate_unchanged()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(LocalName, "k", "v");

        var gate = new RecordingAccessGate();
        var control = _fixture.CreateControlWith(new BackupAccessAuthorizer(gate, membership: null));

        var captured = await control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        Assert.Multiple(() =>
        {
            Assert.That(gate.TreeIdsFor(LatticeOperation.Backup), Is.EqualTo(new[] { LocalName }));
            Assert.That(captured.Manifest.Scope.TreeId, Is.EqualTo(LocalName));
        });
    }

    [Test]
    public async Task With_no_tenancy_add_on_the_probe_returns_the_callers_own_scope_instance()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(LocalName);

        var caps = await _fixture.Control.ProbeCapabilitiesAsync(scope);

        // Reference equality: the warm path rebuilds no selector, so a cluster with
        // tenancy off pays nothing for the composition seam.
        Assert.That(caps.Scope, Is.SameAs(scope));
    }

    [Test]
    public async Task With_the_default_tenant_asserted_the_caller_scope_is_unchanged()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(LocalName);

        var control = _fixture.CreateControlForTenant(new FixedTenantResolver(TenantId.Default));
        var caps = await control.ProbeCapabilitiesAsync(scope);

        Assert.That(caps.Scope, Is.SameAs(scope));
    }

    [Test]
    public async Task A_reserved_tree_name_is_never_composed_even_under_an_active_tenant()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);

        // Already-qualified names and the system-data namespace are governed by
        // their own guards and must pass through untouched (never double-composed).
        await control.ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree("t/globex/orders"));
        await control.ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree("sys-backup-catalog"));

        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Backup),
            Is.EqualTo(new[] { "t/globex/orders", "sys-backup-catalog" }));
    }

    // ---- Resolver contract ----------------------------------------------

    [Test]
    public async Task An_asynchronously_resolving_tenant_still_composes_the_caller_scope()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        var control = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(gate, membership: null),
            new FixedTenantResolver(TenantId.Parse(Acme), resolvesSynchronously: false));

        await control.ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(LocalName));

        Assert.That(gate.TreeIdsFor(LatticeOperation.Backup), Is.EqualTo(new[] { Effective(Acme, LocalName) }));
    }

    [Test]
    public async Task A_caller_with_no_active_tenant_fails_closed_before_the_gate_is_consulted()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        var control = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(gate, membership: null),
            new FixedTenantResolver(default));

        Assert.That(
            async () => await control.CreateBackupAsync(
                new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName))),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());

        // Composition runs before authorization, so a denied tenant never reaches
        // the gate and never names a tree.
        Assert.That(gate.Requests, Is.Empty);
    }

    // ---- Helpers ---------------------------------------------------------

    private static string Effective(string tenant, string name) =>
        LatticeTenantTrees.Compose(TenantId.Parse(tenant), name);

    private ILatticeBackupControl ControlFor(string tenant) =>
        _fixture.CreateControlForTenant(new FixedTenantResolver(TenantId.Parse(tenant)));

    private ILatticeBackupControl ControlFor(string tenant, RecordingAccessGate gate) =>
        _fixture.CreateControlWith(
            new BackupAccessAuthorizer(gate, membership: null),
            new FixedTenantResolver(TenantId.Parse(tenant)));

    /// <summary>
    /// Writes a key into a tree by its effective id. A <c>t/</c>-prefixed id is a
    /// reserved namespace the public surface refuses to create, so the seed runs
    /// under a system-origin scope exactly as the tenancy layer's own composed
    /// routing does.
    /// </summary>
    private async Task SeedAsync(string effectiveTreeId, string key, string value)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.GrainFactory.GetGrain<ILattice>(effectiveTreeId)
                .SetAsync(key, Encoding.UTF8.GetBytes(value));
        }
    }

    private async Task<string?> ReadAsync(string effectiveTreeId, string key)
    {
        var value = await _fixture.GrainFactory.GetGrain<ILattice>(effectiveTreeId).GetAsync(key);
        return value is null ? null : Encoding.UTF8.GetString(value);
    }

    /// <summary>
    /// An <see cref="ITenantContextResolver"/> that always resolves one tenant,
    /// standing in for the tenancy add-on's real context-reading resolver.
    /// <paramref name="resolvesSynchronously"/> selects the warm synchronous path
    /// or forces the asynchronous fallback.
    /// </summary>
    private sealed class FixedTenantResolver(TenantId tenant, bool resolvesSynchronously = true)
        : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(tenant);

        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = resolvesSynchronously ? tenant : default;
            return resolvesSynchronously;
        }
    }

    /// <summary>
    /// An allow-everything gate that records every request it is asked to
    /// authorize, so a test can assert exactly which tree id each facade method
    /// presented - the difference between a composed and an uncomposed scope.
    /// </summary>
    private sealed class RecordingAccessGate : ILatticeAccessGate
    {
        private readonly ConcurrentQueue<LatticeAccessRequest> _requests = new();

        public IReadOnlyList<LatticeAccessRequest> Requests => [.. _requests];

        public IReadOnlyList<LatticeAccessRequest> RequestsFor(LatticeOperation operation) =>
            [.. _requests.Where(r => (r.Operation & operation) == operation)];

        public IReadOnlyList<string> TreeIdsFor(LatticeOperation operation) =>
            [.. RequestsFor(operation).Select(r => r.TreeId)];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            _requests.Enqueue(request);
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
        }
    }
}
