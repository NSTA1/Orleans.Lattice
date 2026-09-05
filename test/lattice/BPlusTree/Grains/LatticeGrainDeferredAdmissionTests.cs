using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the <em>deferred</em> half of the per-tenant write-admission layer: the
/// slow-path continuations the two non-async write entry points
/// (<c>SetAsync</c> / <c>DeleteAsync</c>) fall into when an active
/// <see cref="ITenantAdmissionController"/> returns an admission decision that
/// has not completed synchronously.
/// <para>
/// <see cref="LatticeGrainTenantAdmissionTests"/> uses a controller whose
/// <see cref="ITenantAdmissionController.IsAdmittedAsync"/> returns an
/// already-completed <see cref="ValueTask{T}"/>, so both entry points take their
/// synchronous fast path and the continuations never run. A real controller -
/// one that consults a quota grain or a rate limiter - completes asynchronously,
/// which is the shape modelled here with a controller gated on a
/// <see cref="TaskCompletionSource{TResult}"/>. The contract under test is that
/// deferring the decision changes only <em>when</em> the answer arrives, never
/// what it means: a deferred admit still writes, a deferred refusal still throws,
/// and neither continuation re-runs access-gate enforcement.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainDeferredAdmissionTests
{
    private const string TreeId = "orders";

    [SetUp]
    public void Reset() => LatticeIdempotencyContext.Current = null;

    [TearDown]
    public void Clear() => LatticeIdempotencyContext.Current = null;

    private static LatticeGrain CreateGrain(string treeId, ITenantAdmissionController controller)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITenantAdmissionController)).Returns(controller);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        return new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    /// <summary>
    /// An active admission controller whose decision is deliberately deferred: it
    /// hands back a <see cref="ValueTask{T}"/> over an uncompleted
    /// <see cref="TaskCompletionSource{TResult}"/>, so the caller observes
    /// <c>IsCompletedSuccessfully == false</c> and must take its continuation
    /// path. The test completes the source explicitly, so there is no sleeping
    /// and no timing dependency.
    /// </summary>
    private sealed class DeferredAdmissionController : ITenantAdmissionController
    {
        private readonly TaskCompletionSource<bool> _decision =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool IsActive => true;

        public int CallCount { get; private set; }

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            CallCount++;
            return new ValueTask<bool>(_decision.Task);
        }

        public void Admit() => _decision.TrySetResult(true);

        public void Refuse() => _decision.TrySetResult(false);
    }

    [Test]
    public async Task SetAsync_with_a_deferred_admit_completes_the_write_through_the_continuation()
    {
        var controller = new DeferredAdmissionController();
        var grain = CreateGrain(TreeId, controller);

        var pending = grain.SetAsync("k", [1]);
        Assert.That(pending.IsCompleted, Is.False,
            "an incomplete admission decision must defer the write rather than resolve it synchronously");

        controller.Admit();
        await pending;

        Assert.That(controller.CallCount, Is.EqualTo(1));
    }

    [Test]
    public void SetAsync_with_a_deferred_refusal_still_fails_closed()
    {
        var controller = new DeferredAdmissionController();
        var grain = CreateGrain(TreeId, controller);

        var pending = grain.SetAsync("k", [1]);
        controller.Refuse();

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => pending,
            "deferring the decision must not weaken the refusal into an admit");
    }

    [Test]
    public async Task DeleteAsync_with_a_deferred_admit_completes_the_delete_through_the_continuation()
    {
        var controller = new DeferredAdmissionController();
        var grain = CreateGrain(TreeId, controller);

        var pending = grain.DeleteAsync("k");
        Assert.That(pending.IsCompleted, Is.False);

        controller.Admit();
        await pending;

        Assert.That(controller.CallCount, Is.EqualTo(1));
    }

    [Test]
    public void DeleteAsync_with_a_deferred_refusal_still_fails_closed()
    {
        var controller = new DeferredAdmissionController();
        var grain = CreateGrain(TreeId, controller);

        var pending = grain.DeleteAsync("k");
        controller.Refuse();

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(() => pending);
    }

    [Test]
    public async Task DeleteAsync_with_a_deferred_admit_inside_an_idempotency_scope_routes_through_the_mutation_wrapper()
    {
        var controller = new DeferredAdmissionController();
        var grain = CreateGrain(TreeId, controller);

        using var scope = LatticeIdempotencyContext.With(LatticeIdempotencyKey.Fresh());
        var pending = grain.DeleteAsync("k");
        Assert.That(pending.IsCompleted, Is.False);

        controller.Admit();
        await pending;

        Assert.That(LatticeIdempotencyContext.IsActive, Is.True,
            "the deferred delete must run under the caller's idempotency scope, not outside it");
        Assert.That(controller.CallCount, Is.EqualTo(1));
    }
}
