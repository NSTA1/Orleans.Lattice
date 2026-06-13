using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Verifies that <see cref="ILattice.GetLeafProjectionDigestForRangeAsync"/>
/// fast-fails at the public surface with an <see cref="InvalidOperationException"/>
/// when the per-tree <see cref="LatticeOptions.MaintainProjectionDigest"/>
/// opt-out is set, mirroring <see cref="ILattice.GetLeafProjectionDigestAsync"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeRangeDigestDisabledIntegrationTests
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

    [Test]
    public async Task RangeDigest_throws_when_digest_maintenance_disabled()
    {
        var tree = await _fixture.CreateTreeAsync($"range-disabled-{Guid.NewGuid():N}");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await tree.GetLeafProjectionDigestForRangeAsync(0, null, null));
    }
}
