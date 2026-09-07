using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression tests for the <em>second</em> authorization boundary on the restore
/// engine: the tree the manifest was <b>captured from</b>.
/// <para>
/// A restore names two trees. The target is the tree written into, and it has
/// always been gated with the <c>Restore</c> capability. The source is the tree
/// recorded in the manifest's own <see cref="BackupManifest.Scope"/>, and it was
/// never gated at all - the effective scope is retargeted onto the caller-supplied
/// target tree <em>before</em> the authorization call, so the captured tree id was
/// discarded before anything looked at it. A caller holding <c>Restore</c> on a
/// tree it owns could therefore have the entire contents of a tree it holds
/// nothing on written into a tree it fully controls, gated only on knowing a
/// backup id: a cross-tree read primitive.
/// </para>
/// <para>
/// The fix gates the captured source with the <c>Backup</c> capability - the same
/// capability every other manifest-consuming verb on the backup control facade
/// uses, and the authority the caller would have needed to capture the manifest
/// itself - and only when the restore actually retargets, so the supported
/// same-tree restore is untouched and the supported cross-tree retarget keeps
/// working for a caller that holds both grants.
/// </para>
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupRestoreCapturedSourceAuthorizationTests
{
    private const string Source = "orders";
    private const string Target = "orders-clone";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// An access gate that allows exactly the (tree id, operation) pairs it is
    /// seeded with and denies every other, so a test can grant one identifier of a
    /// two-identifier restore and withhold the other. It records every pair it was
    /// asked about, which is what proves the captured source reached the gate at
    /// all rather than the denial coming from somewhere incidental.
    /// </summary>
    private sealed class PerTreeGate(params (string TreeId, LatticeOperation Operation)[] allowed)
        : ILatticeAccessGate
    {
        private readonly HashSet<(string, LatticeOperation)> _allowed = [.. allowed];

        public List<(string TreeId, LatticeOperation Operation)> Requested { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            var pair = (request.TreeId, request.Operation);
            lock (Requested)
            {
                Requested.Add(pair);
            }

            return new(_allowed.Contains(pair)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny($"no {request.Operation} grant on '{request.TreeId}'"));
        }
    }

    private ILatticeBackupRestoreService RestoreGatedBy(PerTreeGate gate) =>
        _fixture.CreateRestoreServiceWith(new BackupAccessAuthorizer(gate, membership: null));

    private async Task<LatticeBackupCaptureResult> SeedAndCaptureSourceAsync()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        return await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree(Source)));
    }

    // ---- The gap: a retarget never authorized the captured source -------

    [Test]
    public async Task RestoreAsync_denies_a_retarget_whose_captured_source_the_caller_cannot_back_up()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        // The caller owns the target outright - Restore and Backup on it - and holds
        // nothing at all on the tree the backup was captured from.
        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Target, LatticeOperation.Backup));

        Assert.That(
            async () => await RestoreGatedBy(gate).RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, Target)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        // The captured source actually reached the gate, and nothing was written.
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(Target).GetAsync("k1"), Is.Null);
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(Target).GetAsync("k2"), Is.Null);
        });
    }

    [Test]
    public async Task ShadowCutover_restore_denies_a_retarget_whose_captured_source_the_caller_cannot_back_up()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Target, LatticeOperation.Backup));

        Assert.That(
            async () => await RestoreGatedBy(gate).RestoreAsync(
                new LatticeRestoreRequest(
                    backup.BackupId, Target, scope: null, mode: LatticeRestoreMode.ShadowCutover)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
    }

    [Test]
    public async Task BuildShadowAsync_denies_a_retarget_whose_captured_source_the_caller_cannot_back_up()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Target, LatticeOperation.Backup));
        var engine = (ILatticeCoordinatedRestoreEngine)RestoreGatedBy(gate);

        Assert.That(
            async () => await engine.BuildShadowAsync(
                new LatticeRestoreRequest(
                    backup.BackupId, Target, scope: null, mode: LatticeRestoreMode.ShadowCutover)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
    }

    [Test]
    public async Task ColdRestoreAsync_denies_a_retarget_whose_captured_source_the_caller_cannot_back_up()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Target, LatticeOperation.Backup));
        var cold = _fixture.CreateColdRestoreServiceWith(RestoreGatedBy(gate));

        Assert.That(
            async () => await cold.ColdRestoreAsync(new LatticeRestoreRequest(backup.BackupId, Target)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
    }

    [Test]
    public async Task RestoreAsync_denies_a_retarget_whose_chain_base_names_an_unauthorized_source()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        // A tip whose own captured scope names the target - so the tip alone would
        // pass - layered on a base captured from the foreign source tree. The chain
        // is walked from the sink and catalog, which are a trust boundary, so every
        // distinct source in the chain has to be gated, not just the tip's.
        var tip = backup.Manifest with
        {
            Id = "chained-tip",
            Kind = BackupKind.Incremental,
            BaseBackupId = backup.BackupId,
            Scope = BackupScopeSelector.WholeTree(Target),
            ContentDescriptors = [],
            KeyDescriptors = [],
        };
        await _fixture.Catalog.RegisterAsync(tip);

        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Target, LatticeOperation.Backup));

        Assert.That(
            async () => await RestoreGatedBy(gate).RestoreAsync(
                new LatticeRestoreRequest(tip.Id, Target)),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
            Assert.That(await _fixture.GrainFactory.GetGrain<ILattice>(Target).GetAsync("k1"), Is.Null);
        });
    }

    // ---- The supported workflows still work ----------------------------

    [Test]
    public async Task RestoreAsync_allows_a_retarget_when_the_caller_can_back_up_the_captured_source()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        // Clone / disaster-recovery-into-a-new-id / environment-seeding: the caller
        // can restore the target and could have captured the source itself.
        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Source, LatticeOperation.Backup));

        var result = await RestoreGatedBy(gate).RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, Target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(Target);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(2));
            Assert.That(Str((await restored.GetAsync("k1"))!), Is.EqualTo("v1"));
            Assert.That(Str((await restored.GetAsync("k2"))!), Is.EqualTo("v2"));
        });

        Assert.That(gate.Requested, Does.Contain((Source, LatticeOperation.Backup)));
    }

    [Test]
    public async Task RestoreAsync_same_tree_restore_needs_no_backup_grant()
    {
        await _fixture.InitializeAsync();
        var backup = await SeedAndCaptureSourceAsync();

        // The ordinary point-in-time rollback: source and target are the same tree,
        // so there is no second identifier and Restore alone remains sufficient.
        var gate = new PerTreeGate((Source, LatticeOperation.Restore));

        var result = await RestoreGatedBy(gate).RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, Source));

        Assert.Multiple(() =>
        {
            Assert.That(result.TargetTreeId, Is.EqualTo(Source));
            Assert.That(
                gate.Requested,
                Is.EqualTo(new[] { (Source, LatticeOperation.Restore) }),
                "a same-tree restore must not consult the gate for a Backup grant");
        });
    }

    [Test]
    public async Task RestoreAsync_sub_scoped_retarget_authorizes_only_the_replayed_range_of_the_source()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("a:1", Bytes("va1"));
        await source.SetAsync("b:1", Bytes("vb1"));

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree(Source)));

        // The source is gated at the range actually replayed out of it - the prefix
        // the caller asked for - never at the whole captured scope.
        var gate = new PerTreeGate(
            (Target, LatticeOperation.Restore),
            (Source, LatticeOperation.Backup));

        var result = await RestoreGatedBy(gate).RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, Target)
            {
                Scope = BackupScopeSelector.Prefix(Source, "a:"),
            });

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(Target);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(1));
            Assert.That(Str((await restored.GetAsync("a:1"))!), Is.EqualTo("va1"));
            Assert.That(await restored.GetAsync("b:1"), Is.Null);
        });
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);
}
