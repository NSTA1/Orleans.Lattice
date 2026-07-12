using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for <see cref="ILatticeBackupControl.ScheduleBackupAsync"/>: a
/// permitted call registers the matching recurring reminder on the per-scope
/// scheduler grain, a denying gate fails the call closed before any reminder is
/// registered, and a null request is rejected.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlScheduleTests
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
    public async Task ScheduleBackupAsync_registers_the_full_reminder_for_the_scope()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);

        await _fixture.Control.ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(scope, incremental: false, TimeSpan.FromMinutes(20)));

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        var hasFull = await grain.HasScheduleAsync(incremental: false);
        var hasIncremental = await grain.HasScheduleAsync(incremental: true);
        Assert.Multiple(() =>
        {
            Assert.That(hasFull, Is.True);
            Assert.That(hasIncremental, Is.False);
        });
    }

    [Test]
    public async Task ScheduleBackupAsync_registers_the_incremental_reminder_when_requested()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);

        await _fixture.Control.ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(scope, incremental: true, TimeSpan.FromMinutes(45)));

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        Assert.That(await grain.HasScheduleAsync(incremental: true), Is.True);
    }

    [Test]
    public async Task ScheduleBackupAsync_denied_permission_fails_closed()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no backup grant"), membership: null));

        Assert.That(
            async () => await denying.ScheduleBackupAsync(
                new LatticeBackupScheduleRequest(scope, incremental: false, TimeSpan.FromMinutes(20))),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        var grain = _fixture.GrainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        Assert.That(await grain.HasScheduleAsync(incremental: false), Is.False);
    }

    [Test]
    public async Task ScheduleBackupAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.ScheduleBackupAsync(null!),
            Throws.ArgumentNullException);
    }

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
