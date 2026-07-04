namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Verifies the zero-cost default: when the authorization add-on is <b>not</b>
/// registered (the core default no-op access gate is in place), the state API
/// performs no auth-backed read filtering and no caller-subject resolution, so
/// its behaviour is exactly as before issue #981. Uses the plain, auth-free
/// <see cref="StateQueryClusterFixture"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateZeroCostTests
{
    private StateQueryClusterFixture _fixture = null!;

    private const string Tree = "zerocost-tree";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new StateQueryClusterFixture();
        await _fixture.InitializeAsync();
        await _fixture.CreatePopulatedTreeAsync(Tree, keyCount: 5);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public void Visibility_filter_is_disabled_when_no_auth_gate_is_registered()
    {
        // The resolved filter sees the core NullLatticeAccessGate, so it is off:
        // no subject is ever resolved on the read path.
        var filter = new LatticeStateVisibilityFilter(
            _fixture.SiloServices,
            new LatticeApiStateOptions());

        Assert.That(filter.Enabled, Is.False);
    }

    [Test]
    public async Task ListTrees_returns_the_tree_with_no_identity_present()
    {
        // No ambient credential at all: without auth this is a normal read.
        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

        Assert.That(page.Entries.Select(e => e.TreeId), Does.Contain(Tree));
    }

    [Test]
    public async Task ScanEntries_returns_every_key_with_no_identity_present()
    {
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = Tree,
            PageSize = 50,
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entries, Has.Count.EqualTo(5));
    }
}
