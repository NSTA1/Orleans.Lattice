using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the clamping computed properties (<c>EffectivePageSize</c>,
/// <c>EffectiveDepthLimit</c>, <c>EffectiveMaxNodes</c>) on the paging / query
/// request records. Each property is a three-arm switch (below-minimum falls
/// back to a default, above-maximum clamps down, in-range passes through); every
/// arm is covered so no branch is missed.
/// </summary>
[TestFixture]
public class RequestClampingTests
{
    [Test]
    public void CatalogRequest_EffectivePageSize_clamps_all_three_arms()
    {
        Assert.That(new CatalogRequest { PageSize = 0 }.EffectivePageSize,
            Is.EqualTo(CatalogRequest.DefaultPageSize));
        Assert.That(new CatalogRequest { PageSize = CatalogRequest.MaxPageSize + 1 }.EffectivePageSize,
            Is.EqualTo(CatalogRequest.MaxPageSize));
        Assert.That(new CatalogRequest { PageSize = 42 }.EffectivePageSize, Is.EqualTo(42));
    }

    [Test]
    public void DeadLetterQueueRequest_EffectivePageSize_clamps_all_three_arms()
    {
        Assert.That(new DeadLetterQueueRequest { TreeId = "t", PageSize = -1 }.EffectivePageSize,
            Is.EqualTo(DeadLetterQueueRequest.DefaultPageSize));
        Assert.That(new DeadLetterQueueRequest { TreeId = "t", PageSize = DeadLetterQueueRequest.MaxPageSize + 5 }
            .EffectivePageSize, Is.EqualTo(DeadLetterQueueRequest.MaxPageSize));
        Assert.That(new DeadLetterQueueRequest { TreeId = "t", PageSize = 250 }.EffectivePageSize,
            Is.EqualTo(250));
    }

    [Test]
    public void TagMemberScanRequest_EffectivePageSize_clamps_all_three_arms()
    {
        Assert.That(new TagMemberScanRequest { IndexName = "i", Tag = "t", PageSize = 0 }.EffectivePageSize,
            Is.EqualTo(TagMemberScanRequest.DefaultPageSize));
        Assert.That(new TagMemberScanRequest { IndexName = "i", Tag = "t", PageSize = TagMemberScanRequest.MaxPageSize + 1 }
            .EffectivePageSize, Is.EqualTo(TagMemberScanRequest.MaxPageSize));
        Assert.That(new TagMemberScanRequest { IndexName = "i", Tag = "t", PageSize = 7 }.EffectivePageSize,
            Is.EqualTo(7));
    }

    [Test]
    public void AuthPageRequest_EffectivePageSize_clamps_all_three_arms()
    {
        Assert.That(new AuthPageRequest { PageSize = -10 }.EffectivePageSize,
            Is.EqualTo(AuthPageRequest.DefaultPageSize));
        Assert.That(new AuthPageRequest { PageSize = AuthPageRequest.MaxPageSize + 100 }.EffectivePageSize,
            Is.EqualTo(AuthPageRequest.MaxPageSize));
        Assert.That(new AuthPageRequest { PageSize = 500 }.EffectivePageSize, Is.EqualTo(500));
    }

    [Test]
    public void StructureRequest_EffectiveDepthLimit_clamps_all_three_arms()
    {
        Assert.That(new StructureRequest { TreeId = "t", DepthLimit = -1 }.EffectiveDepthLimit,
            Is.EqualTo(StructureRequest.DefaultDepthLimit));
        Assert.That(new StructureRequest { TreeId = "t", DepthLimit = StructureRequest.MaxDepthLimit + 1 }
            .EffectiveDepthLimit, Is.EqualTo(StructureRequest.MaxDepthLimit));
        Assert.That(new StructureRequest { TreeId = "t", DepthLimit = 8 }.EffectiveDepthLimit, Is.EqualTo(8));
    }

    [Test]
    public void StructureRequest_EffectiveMaxNodes_clamps_all_three_arms()
    {
        Assert.That(new StructureRequest { TreeId = "t", MaxNodes = 0 }.EffectiveMaxNodes,
            Is.EqualTo(StructureRequest.DefaultMaxNodes));
        Assert.That(new StructureRequest { TreeId = "t", MaxNodes = StructureRequest.MaxNodeBudget + 1 }
            .EffectiveMaxNodes, Is.EqualTo(StructureRequest.MaxNodeBudget));
        Assert.That(new StructureRequest { TreeId = "t", MaxNodes = 5000 }.EffectiveMaxNodes, Is.EqualTo(5000));
    }
}
