using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

public class InternalGrainGuardFilterTests
{
    private static readonly InternalGrainGuardFilter Filter = new();

    [SetUp]
    public void ClearRequestContext()
    {
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
    }

    internal static IIncomingGrainCallContext CreateCallContext(object grainInstance)
    {
        var ctx = Substitute.For<IIncomingGrainCallContext>();
        ctx.Invoke().Returns(Task.CompletedTask);
        var grainContext = Substitute.For<IGrainContext>();
        grainContext.GrainInstance.Returns(grainInstance);
        grainContext.GrainId.Returns(GrainId.Create("test", "key"));
        ctx.TargetContext.Returns(grainContext);
        return ctx;
    }

    [Test]
    public async Task Invoke_allows_ILattice_from_client()
    {
        var callCtx = CreateCallContext(Substitute.For<ILattice>());

        await Filter.Invoke(callCtx);

        await callCtx.Received(1).Invoke();
    }

    [Test]
    public void Invoke_blocks_IShardRootGrain_without_token()
    {
        var callCtx = CreateCallContext(Substitute.For<IShardRootGrain>());

        Assert.ThrowsAsync<InvalidOperationException>(() => Filter.Invoke(callCtx));
    }

    [Test]
    public async Task Invoke_allows_IShardRootGrain_with_token()
    {
        var callCtx = CreateCallContext(Substitute.For<IShardRootGrain>());
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, LatticeConstants.InternalCallTokenValue);

        try
        {
            await Filter.Invoke(callCtx);
            await callCtx.Received(1).Invoke();
        }
        finally
        {
            RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
        }
    }

    [Test]
    public void Invoke_blocks_IBPlusLeafGrain_without_token()
    {
        var callCtx = CreateCallContext(Substitute.For<IBPlusLeafGrain>());

        Assert.ThrowsAsync<InvalidOperationException>(() => Filter.Invoke(callCtx));
    }

    [Test]
    public void Invoke_rejects_wrong_token_value()
    {
        var callCtx = CreateCallContext(Substitute.For<IShardRootGrain>());
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, "wrong");

        try
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => Filter.Invoke(callCtx));
        }
        finally
        {
            RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
        }
    }
}

public class LatticeCallContextFilterTests
{
    private static LatticeCallContextFilter CreateFilter(object? grainInstance)
    {
        var grainContext = Substitute.For<IGrainContext>();
        grainContext.GrainInstance.Returns(grainInstance);
        var accessor = Substitute.For<IGrainContextAccessor>();
        accessor.GrainContext.Returns(grainContext);
        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IGrainContextAccessor)).Returns(accessor);
        return new LatticeCallContextFilter(serviceProvider);
    }

    private static IOutgoingGrainCallContext CreateOutgoingContext()
    {
        var ctx = Substitute.For<IOutgoingGrainCallContext>();
        ctx.Invoke().Returns(Task.CompletedTask);
        return ctx;
    }

    [TearDown]
    public void ClearToken()
    {
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
    }

    [SetUp]
    public void ClearTokenBefore()
    {
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, null!);
    }

    [Test]
    public async Task Invoke_stamps_token_for_lattice_grain()
    {
        var filter = CreateFilter(Substitute.For<IShardRootGrain>());
        var ctx = CreateOutgoingContext();

        await filter.Invoke(ctx);

        Assert.That(
            RequestContext.Get(LatticeConstants.InternalCallTokenKey),
            Is.EqualTo(LatticeConstants.InternalCallTokenValue));
    }

    [Test]
    public async Task Invoke_stamps_token_for_ILattice()
    {
        var filter = CreateFilter(Substitute.For<ILattice>());
        var ctx = CreateOutgoingContext();

        await filter.Invoke(ctx);

        Assert.That(
            RequestContext.Get(LatticeConstants.InternalCallTokenKey),
            Is.EqualTo(LatticeConstants.InternalCallTokenValue));
    }

    [Test]
    public async Task Invoke_does_not_stamp_token_for_non_lattice_grain()
    {
        var filter = CreateFilter(Substitute.For<IGrainWithStringKey>());
        var ctx = CreateOutgoingContext();

        await filter.Invoke(ctx);

        Assert.That(RequestContext.Get(LatticeConstants.InternalCallTokenKey), Is.Null);
    }

    [Test]
    public async Task Invoke_clears_leaked_token_for_non_lattice_grain()
    {
        // Simulate a token that leaked from a previous Lattice grain call.
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, LatticeConstants.InternalCallTokenValue);

        var filter = CreateFilter(Substitute.For<IGrainWithStringKey>());
        var ctx = CreateOutgoingContext();

        await filter.Invoke(ctx);

        Assert.That(RequestContext.Get(LatticeConstants.InternalCallTokenKey), Is.Null);
    }

    [Test]
    public async Task Token_does_not_leak_through_non_lattice_intermediary()
    {
        // Step 1: Lattice grain stamps the token on an outgoing call.
        var latticeFilter = CreateFilter(Substitute.For<IShardRootGrain>());
        await latticeFilter.Invoke(CreateOutgoingContext());
        Assert.That(
            RequestContext.Get(LatticeConstants.InternalCallTokenKey),
            Is.EqualTo(LatticeConstants.InternalCallTokenValue));

        // Step 2: Non-Lattice intermediary grain makes an outgoing call —
        // the filter must clear the token so the next hop cannot bypass the guard.
        var externalFilter = CreateFilter(Substitute.For<IGrainWithStringKey>());
        await externalFilter.Invoke(CreateOutgoingContext());
        Assert.That(RequestContext.Get(LatticeConstants.InternalCallTokenKey), Is.Null);

        // Step 3: The guarded grain should reject the call since the token was cleared.
        var incomingFilter = new InternalGrainGuardFilter();
        var incomingCtx = InternalGrainGuardFilterTests.CreateCallContext(Substitute.For<IShardRootGrain>());

        Assert.ThrowsAsync<InvalidOperationException>(() => incomingFilter.Invoke(incomingCtx));
    }
}
