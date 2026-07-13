using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the read-only capability probe on the
/// <see cref="ILatticeBackupControl"/> facade:
/// <see cref="ILatticeBackupControl.ProbeCapabilitiesAsync"/> evaluates the
/// caller's backup / restore authority for a scope with no side effects and
/// never throws on a permission denial - a denied capability is reported as a
/// <c>false</c> flag (default-deny). The capture / list / incremental / delete
/// flags reflect the scope's capture (backup) grant; the restore flag reflects
/// the scope's author (restore) grant.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlCapabilityTests
{
    private const string Source = "orders";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ProbeCapabilitiesAsync_all_granted_reports_every_capability()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);

        var caps = await _fixture.Control.ProbeCapabilitiesAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(caps.Scope, Is.EqualTo(scope));
            Assert.That(caps.CanList, Is.True);
            Assert.That(caps.CanCapture, Is.True);
            Assert.That(caps.CanCaptureIncremental, Is.True);
            Assert.That(caps.CanRestore, Is.True);
            Assert.That(caps.CanDelete, Is.True);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_fully_denied_reports_no_capabilities_without_throwing()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no grant"), membership: null));

        var caps = await denying.ProbeCapabilitiesAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(caps.Scope, Is.EqualTo(scope));
            Assert.That(caps.CanList, Is.False);
            Assert.That(caps.CanCapture, Is.False);
            Assert.That(caps.CanCaptureIncremental, Is.False);
            Assert.That(caps.CanRestore, Is.False);
            Assert.That(caps.CanDelete, Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_backup_granted_restore_denied_reports_split()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var split = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new OperationScopedGate(LatticeOperation.Backup), membership: null));

        var caps = await split.ProbeCapabilitiesAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanList, Is.True);
            Assert.That(caps.CanCapture, Is.True);
            Assert.That(caps.CanCaptureIncremental, Is.True);
            Assert.That(caps.CanDelete, Is.True);
            Assert.That(caps.CanRestore, Is.False);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_restore_granted_backup_denied_reports_split()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var split = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new OperationScopedGate(LatticeOperation.Restore), membership: null));

        var caps = await split.ProbeCapabilitiesAsync(scope);

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanList, Is.False);
            Assert.That(caps.CanCapture, Is.False);
            Assert.That(caps.CanCaptureIncremental, Is.False);
            Assert.That(caps.CanDelete, Is.False);
            Assert.That(caps.CanRestore, Is.True);
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_null_scope_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.ProbeCapabilitiesAsync(null!),
            Throws.ArgumentNullException);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>A gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }

    /// <summary>A gate that permits only requests whose operation matches the granted mask.</summary>
    private sealed class OperationScopedGate(LatticeOperation granted) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new((request.Operation & granted) == granted
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("operation not granted"));
    }
}
