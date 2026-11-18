using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for <see cref="ILatticeBackupControl.GetBackupSetScheduleProgressAsync"/>,
/// the lightweight read the Explorer polls while a backup-set schedule's run is in
/// flight: it answers <see langword="null"/> for an idle schedule, throws
/// <see cref="KeyNotFoundException"/> for an unknown one, and fails closed under a
/// denying gate exactly as the full status read does.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlSetScheduleProgressTests
{
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
    public async Task GetBackupSetScheduleProgressAsync_returns_null_when_no_run_is_in_flight()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("idle", a, b));

        var progress = await _fixture.Control.GetBackupSetScheduleProgressAsync("idle");

        Assert.That(progress, Is.Null);
    }

    [Test]
    public async Task GetBackupSetScheduleProgressAsync_throws_for_an_unknown_schedule()
    {
        await _fixture.InitializeAsync();

        Assert.That(
            async () => await _fixture.Control.GetBackupSetScheduleProgressAsync("absent"),
            Throws.InstanceOf<KeyNotFoundException>());
    }

    [Test]
    public async Task GetBackupSetScheduleProgressAsync_fails_closed_under_a_denying_gate()
    {
        await _fixture.InitializeAsync();
        var (a, b) = await SeedAsync();
        await _fixture.Control.ScheduleBackupSetAsync(Request("guarded", a, b));
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no backup grant"), membership: null));

        Assert.That(
            async () => await denying.GetBackupSetScheduleProgressAsync("guarded"),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    private async Task<(string A, string B)> SeedAsync()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var a = $"progress-a-{suffix}";
        var b = $"progress-b-{suffix}";
        await _fixture.GrainFactory.GetGrain<ILattice>(a).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await _fixture.GrainFactory.GetGrain<ILattice>(b).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        return (a, b);
    }

    private static LatticeBackupSetScheduleRequest Request(string name, string a, string b) =>
        LatticeBackupSetScheduleRequest.ForTrees(
            name,
            [BackupScopeSelector.WholeTree(a), BackupScopeSelector.WholeTree(b)],
            TimeSpan.FromHours(6),
            retentionKeepLast: 3,
            retentionMaxAge: null);

    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
