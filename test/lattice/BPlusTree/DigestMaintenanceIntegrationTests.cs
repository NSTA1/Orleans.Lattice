using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> opt-out at the
/// public-surface (<see cref="ILattice.GetLeafProjectionDigestAsync"/>)
/// boundary. Verifies that disabling maintenance fast-fails the digest
/// API across the full grain stack while leaving every other tree
/// operation intact.
/// </summary>
[TestFixture]
[Category("Integration")]
public class DigestMaintenanceIntegrationTests
{
    private DigestDisabledClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new DigestDisabledClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");

    [Test]
    public async Task GetLeafProjectionDigestAsync_throws_when_maintenance_disabled()
    {
        var tree = await NewTreeAsync("digest-off");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await tree.GetLeafProjectionDigestAsync(0));

        Assert.That(ex!.Message, Does.Contain(nameof(LatticeOptions.MaintainProjectionDigest)));
        Assert.That(ex.Message, Does.Contain("disabled"));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_throws_after_writes_when_disabled()
    {
        // Confirm the fast-fail still applies after the tree has accepted
        // mutations: maintenance is off, so the persisted aggregate is
        // not the source of truth and the API stays unavailable.
        var tree = await NewTreeAsync("digest-off-writes");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await tree.GetLeafProjectionDigestAsync(0));
    }

    [Test]
    public async Task Writes_and_reads_still_work_when_digest_disabled()
    {
        // The opt-out only affects the digest API; ordinary CRUD must
        // remain unchanged so the operator-facing trade-off is purely
        // "no digest" rather than "no tree".
        var tree = await NewTreeAsync("digest-off-crud");

        await tree.SetAsync("hello", Encoding.UTF8.GetBytes("world"));
        var value = await tree.GetAsync("hello");
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("world"));

        await tree.DeleteAsync("hello");
        Assert.That(await tree.GetAsync("hello"), Is.Null);
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_cancellation_throws_before_maintain_check()
    {
        // A pre-cancelled token must surface as OperationCanceledException,
        // not InvalidOperationException - the cancellation check runs
        // ahead of the maintain-flag fast-fail so chaos-test harnesses
        // observe the cooperative-cancel contract regardless of the
        // opt-out's value.
        var tree = await NewTreeAsync("digest-off-cancel");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await tree.GetLeafProjectionDigestAsync(0, cts.Token));
    }
}
