using System.Reflection;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the default implementation of
/// <see cref="ILatticeDataApi.ProbeCapabilitiesAsync"/>, the write-capability
/// probe added to the data contract after it had shipped (#5367).
/// </summary>
/// <remarks>
/// <para>
/// The member is a default interface method so that an implementer written
/// against the earlier contract keeps compiling. Its default must fail
/// <em>closed</em>: a probe that cannot answer reports no write and no delete, so
/// a management surface hides its write controls rather than offering writes it
/// has no evidence the caller may make. The writes themselves are still
/// authorized on attempt.
/// </para>
/// <para>
/// Every production implementer overrides the member, so a compiled legacy
/// implementer is the only caller that reaches the default.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DataApiProbeDefaultMemberTests
{
    [Test]
    public void ProbeCapabilitiesAsync_is_a_default_member_so_a_legacy_implementer_still_compiles()
    {
        var declared = typeof(ILatticeDataApi).GetMethod(nameof(ILatticeDataApi.ProbeCapabilitiesAsync));

        Assert.That(declared, Is.Not.Null);
        Assert.That(
            declared!.IsAbstract,
            Is.False,
            "ProbeCapabilitiesAsync must stay a default interface member. Were it abstract, every pre-existing "
            + "implementer of the released contract would stop compiling.");
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_fails_closed_on_an_implementer_that_predates_it()
    {
        var legacyType = LegacyContractImplementer.Compile(typeof(ILatticeDataApi), nameof(ILatticeDataApi.ProbeCapabilitiesAsync));
        var legacy = (ILatticeDataApi)Activator.CreateInstance(legacyType)!;

        Assert.That(
            legacyType.GetMethod(nameof(ILatticeDataApi.ProbeCapabilitiesAsync), BindingFlags.Public | BindingFlags.Instance),
            Is.Null,
            "the generated implementer must inherit the default rather than declare its own, or this proves nothing");

        var whole = await legacy.ProbeCapabilitiesAsync("orders");
        var point = await legacy.ProbeCapabilitiesAsync("orders", "order/1");

        Assert.Multiple(() =>
        {
            Assert.That(whole, Is.EqualTo(new LatticeDataCapabilities { TreeId = "orders" }));
            Assert.That(point, Is.EqualTo(new LatticeDataCapabilities { TreeId = "orders", Key = "order/1" }));
            Assert.That(whole.CanWrite || whole.CanDelete || point.CanWrite || point.CanDelete, Is.False);
        });
    }

    [Test]
    public void ProbeCapabilitiesAsync_default_rejects_a_missing_tree_id()
    {
        var legacy = (ILatticeDataApi)LegacyContractImplementer.CreateInstance(typeof(ILatticeDataApi));

        Assert.Multiple(() =>
        {
            Assert.That(() => legacy.ProbeCapabilitiesAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => legacy.ProbeCapabilitiesAsync(null!), Throws.ArgumentNullException);
        });
    }
}
