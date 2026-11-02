namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// The data-plane capability probe (#5367) on a host with no authorization add-on:
/// nothing gates a write there, so the probe reports every write allowed.
/// </summary>
public sealed partial class NoAuthApiDataTests
{
    [Test]
    public async Task probe_allows_every_write_when_no_gate_enforces()
    {
        const string tree = "noauth-probe";
        await _fixture.RegisterTreeAsync(tree);

        var whole = await _fixture.Api.ProbeCapabilitiesAsync(tree);
        var point = await _fixture.Api.ProbeCapabilitiesAsync(tree, "k1");

        Assert.Multiple(() =>
        {
            Assert.That(whole, Is.EqualTo(new LatticeDataCapabilities { TreeId = tree, CanWrite = true, CanDelete = true }));
            Assert.That(point, Is.EqualTo(new LatticeDataCapabilities { TreeId = tree, Key = "k1", CanWrite = true, CanDelete = true }));
        });
    }
}
