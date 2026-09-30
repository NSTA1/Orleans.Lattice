using NSubstitute;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="GrainTenantPolicyEpochPublisher"/>, the production
/// bridge from a committing silo's change-feed hook to the cluster-wide
/// <see cref="ITenantPolicyEpochGrain"/>.
/// </summary>
[TestFixture]
public sealed class GrainTenantPolicyEpochPublisherTests
{
    [Test]
    public void Constructor_null_grain_factory_throws()
    {
        Assert.That(() => new GrainTenantPolicyEpochPublisher(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task AdvanceAsync_advances_the_single_epoch_grain()
    {
        var grain = Substitute.For<ITenantPolicyEpochGrain>();
        grain.AdvanceAsync().Returns(new TenantPolicyEpoch(Guid.NewGuid(), 1));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITenantPolicyEpochGrain>(ITenantPolicyEpochGrain.Key, Arg.Any<string?>()).Returns(grain);

        await new GrainTenantPolicyEpochPublisher(factory).AdvanceAsync(CancellationToken.None);

        await grain.Received(1).AdvanceAsync();
    }

    [Test]
    public void AdvanceAsync_caller_cancellation_abandons_the_wait()
    {
        var grain = Substitute.For<ITenantPolicyEpochGrain>();
        grain.AdvanceAsync().Returns(new TaskCompletionSource<TenantPolicyEpoch>().Task);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITenantPolicyEpochGrain>(ITenantPolicyEpochGrain.Key, Arg.Any<string?>()).Returns(grain);
        using var cts = new CancellationTokenSource();
        var advance = new GrainTenantPolicyEpochPublisher(factory).AdvanceAsync(cts.Token);

        cts.Cancel();

        Assert.That(async () => await advance, Throws.InstanceOf<OperationCanceledException>());
    }
}
