using Orleans.Core.Internal;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task A_failed_reconcile_after_a_superseded_joint_flip_is_retried_until_it_repairs_the_ordinary_write(bool deactivateBeforeRetry)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-recon-a-src-{suffix}";
        var treeB = $"mv-xt-recon-b-src-{suffix}";
        var viewA = $"mv-xt-recon-a-view-{suffix}";
        var viewB = $"mv-xt-recon-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka", Person(30, "a1"), treeB, "kb", Person(40, "b1"), $"mv-xt-recon-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();

        // The stale joint registration resolves before the ordinary successor
        // becomes public. Conservative reconcile still retries durably, but its
        // failure must not roll that public successor back.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Person(50, "a2"));
        CrossTreeRegisterReadyHookFilter.BeforeNextRegistration(
            viewA,
            grains => grains.GetGrain<IViewMaintainerGrain>(viewB).DrainAsync());
        var failures = 0;
        var failedAttempts = 0;
        var failing = true;
        ViewMaintainerGrain.BeforeCrossTreeReconcileForTest = name =>
        {
            if (name == viewA)
            {
                Interlocked.Increment(ref failures);
                if (Volatile.Read(ref failing))
                {
                    throw new InvalidOperationException("Injected reconcile failure.");
                }
            }

            return Task.CompletedTask;
        };
        try
        {
            await maintainerA.DrainAsync();
            for (var pass = 0; pass < 4 && failures == 0; pass++)
            {
                await maintainerB.DrainAsync();
                await maintainerA.DrainAsync();
            }
            Assert.That(failures, Is.EqualTo(1), "The superseded joint flip scheduled a reconcile, which failed.");
            Assert.That(await (await _fixture.ActiveViewTreeAsync(viewA)).GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "The failed reconcile cannot undo the ordinary survivor published after the joint rendezvous.");

            // Repair retirement advanced the checkpoint before the optional
            // reconcile. Its failure must remain durably retryable nonetheless.
            for (var i = 0; i < 2; i++)
            {
                await maintainerB.DrainAsync();
                await maintainerA.DrainAsync();
            }

            if (deactivateBeforeRetry)
            {
                await maintainerA.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
                maintainerA = await MaintainerAsync(viewA);
            }

            Volatile.Write(ref failing, false);
            failedAttempts = failures;
            for (var i = 0; i < 2; i++)
            {
                await maintainerB.DrainAsync();
                await maintainerA.DrainAsync();
            }
        }
        finally
        {
            ViewMaintainerGrain.BeforeCrossTreeReconcileForTest = null;
        }

        var viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(failures, Is.GreaterThan(failedAttempts), "The failed reconcile must be retried by a later pass.");
            Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "A failed or interrupted reconcile must not leave the stale atomic value over the ordinary write.");
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
        });
    }
}
