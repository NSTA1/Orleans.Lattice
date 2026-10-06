using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4549 through the receiver's real applier. A third origin's write that
/// the applier admits, and stamps with the floor epoch it was admitted under,
/// before a bootstrap installs the drop floor, then reaches its shard root only
/// after the install has armed it. The shard root refuses the stale stamp, and
/// the applier turns the refusal into a deferral, so the sender re-ships the
/// write and the re-delivery is admitted against the new floor and dropped. The
/// write is held between the applier and the shard by an outgoing call filter
/// (<see cref="ApplyHoldFilter"/>), so both of the applier's halves are
/// exercised: the stamp, and the mapping of the refusal.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    [Test]
    public async Task A_write_the_applier_admitted_before_the_floor_is_deferred_when_its_shard_refuses_it_after_the_install()
    {
        const string tree = "rsdr-4549-applier-straddler";
        const string key = "third-applier-straddler";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // C's write reached the source, which deleted the key and reaped it. The
        // copy bound for the receiver is still in flight.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        // The receiver's applier admits the copy, stamps it, and sends it on;
        // the filter holds it before it reaches the shard root.
        Task<ApplyResult> delivery;
        using (var hold = ApplyHoldFilter.Hold(tree, key))
        {
            delivery = DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written);
            var reached = await Task.WhenAny(hold.Reached, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(reached, Is.SameAs(hold.Reached), "precondition: the applier sent the write on before the install");

            // The bootstrap installs the floor and arms every shard root, then
            // runs its reconcile scan, while the write is held.
            await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));
        }

        var held = await delivery;
        var reShipped = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written);

        Assert.Multiple(async () =>
        {
            Assert.That(held.Applied, Is.False);
            Assert.That(held.Deferred, Is.True,
                "the applier stamped the write with the epoch it admitted it under, and turns the shard root's refusal into a deferral");
            Assert.That(reShipped.Applied, Is.False);
            Assert.That(reShipped.Deferred, Is.False, "the re-shipped write is admitted against the final floor and dropped");
            Assert.That(await siteB.GetAsync(key), Is.Null, "the write must not resurrect the key after the scan");
        });
    }

    /// <summary>
    /// Holds the first <see cref="IReplicationApplyGrain.ApplySetAsync"/> call
    /// to one key of one tree on site B until the hold is released (disposed).
    /// Every other call passes straight through.
    /// </summary>
    private sealed class ApplyHoldFilter : IOutgoingGrainCallFilter
    {
        private static Handle? _active;

        public static Handle Hold(string tree, string key)
        {
            var handle = new Handle(tree, key);
            if (Interlocked.CompareExchange(ref _active, handle, null) is not null)
            {
                throw new InvalidOperationException("Only one apply hold may be active at a time.");
            }

            return handle;
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (Volatile.Read(ref _active) is { } handle
                && context.MethodName == nameof(IReplicationApplyGrain.ApplySetAsync)
                && string.Equals(context.TargetId.Key.ToString(), handle.Tree, StringComparison.Ordinal)
                && context.Request.GetArgument(0) is string key
                && string.Equals(key, handle.Key, StringComparison.Ordinal)
                && handle.Claim())
            {
                await handle.WaitForReleaseAsync();
            }

            await context.Invoke();
        }

        public sealed class Handle(string tree, string key) : IDisposable
        {
            private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _claimed;

            public string Tree { get; } = tree;

            public string Key { get; } = key;

            /// <summary>Completes once the held call has reached the filter.</summary>
            public Task Reached => _reached.Task;

            public bool Claim() => Interlocked.Exchange(ref _claimed, 1) == 0;

            public Task WaitForReleaseAsync()
            {
                _reached.TrySetResult();
                return _released.Task;
            }

            public void Dispose()
            {
                Interlocked.CompareExchange(ref _active, null, this);
                _released.TrySetResult();
            }
        }
    }
}
