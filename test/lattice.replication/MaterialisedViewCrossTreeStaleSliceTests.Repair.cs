using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    [Test]
    public async Task A_submitted_repair_pins_its_physical_source_and_generation_until_checkpoint_retirement()
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-repair-held-a-{suffix}";
        var treeB = $"mv-repair-held-b-{suffix}";
        var shadow = $"mv-repair-held-shadow-{suffix}";
        var viewA = $"mv-repair-held-view-a-{suffix}";
        var viewB = $"mv-repair-held-view-b-{suffix}";
        var operationId = $"mv-repair-held-op-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var a = await MaintainerAsync(viewA);
        var b = await MaintainerAsync(viewB);
        var originalA = Person(30, "original-a");
        var replacementA = Person(50, "replacement-a");
        var replacementB = Person(60, "replacement-b");
        await CrossTreeWriteAsync(treeA, "ka", originalA, treeB, "kb", Person(40, "original-b"), operationId);
        await a.DrainAsync();
        CrossTreeRegisterReadyHookFilter.AfterNextRegistration(viewB, async grains =>
        {
            await grains.GetGrain<ILattice>(treeA).SetAsync("ka", replacementA);
            await grains.GetGrain<ILattice>(treeB).SetAsync("kb", replacementB);
        });
        await b.DrainAsync();
        var coordinator = _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId);
        ViewCrossTreeReadiness? captured = null;
        for (var pass = 0; pass < 4 && captured is null; pass++)
        {
            await a.DrainAsync();
            captured = await coordinator.GetRepairReadinessAsync(viewA);
        }

        Assert.That(captured, Is.Not.Null, "A is frozen while B has not joined repair.");
        var heldTree = await _fixture.ActiveViewTreeAsync(viewA);
        var restoredA = Person(90, "restored-a");
        await _fixture.Cluster.Client.GetGrain<ILattice>(shadow).SetAsync("ka", restoredA);
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(treeA, shadow);
        await a.NotifySourceIdentityChangedAsync(shadow);
        Assert.ThrowsAsync<InvalidOperationException>(() => a.RebuildAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => a.ReconcileAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => a.DecommissionAsync());
        await a.DrainAsync();
        Assert.That(await heldTree.GetAsync("ka"), Is.EqualTo(originalA),
            "Neither alias healing nor operator maintenance may rewrite a still-submitted generation.");
        Assert.That((await coordinator.GetRepairReadinessAsync(viewA))!.ViewTreeId, Is.EqualTo(captured!.ViewTreeId));

        await b.DrainAsync();
        Assert.That(await heldTree.GetAsync("ka"), Is.EqualTo(replacementA));
        await a.DrainAsync();
        Assert.That(await heldTree.GetAsync("ka"), Is.EqualTo(replacementA),
            "Even after joint repair, rebinding waits for the old physical checkpoint to retire.");
        for (var pass = 0; pass < 8; pass++)
        {
            await a.DrainAsync();
            await b.DrainAsync();
        }

        var current = await _fixture.ActiveViewTreeAsync(viewA);
        Assert.That(await current.GetAsync("ka"), Is.EqualTo(restoredA), "Retirement permits healing onto the new physical source.");
        Assert.That(await a.GetLagAsync(), Is.Zero);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Joint_repair_fences_ordinary_successors_and_survives_lost_completion_and_acknowledgement(bool reactivate)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-repair-a-source-{suffix}";
        var treeB = $"mv-repair-b-source-{suffix}";
        var viewA = $"mv-repair-a-view-{suffix}";
        var viewB = $"mv-repair-b-view-{suffix}";
        var operationId = $"mv-repair-source-operation-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);
        var originalA = Person(30, "original-a");
        var originalB = Person(40, "original-b");
        var replacementA = Person(50, "replacement-a");
        var replacementB = Person(60, "replacement-b");
        Assert.That(await CrossTreeWriteAsync(treeA, "ka", originalA, treeB, "kb", originalB, operationId),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();
        CrossTreeRegisterReadyHookFilter.AfterNextRegistration(viewB, async grains =>
        {
            await grains.GetGrain<ILattice>(treeA).SetAsync("ka", replacementA);
            await grains.GetGrain<ILattice>(treeB).SetAsync("kb", replacementB);
        });

        var actualA = await _fixture.ActiveViewTreeAsync(viewA);
        var actualB = await _fixture.ActiveViewTreeAsync(viewB);
        var peerRepairSubmitted = false;
        byte[]? peerBeforeA = null;
        byte[]? peerBeforeB = null;
        CrossTreeRegisterReadyHookFilter.BeforeNextRepair(viewB, async _ =>
        {
            // The original joint decision is applied, but neither locally
            // derived repair contribution has permission to publish alone.
            peerBeforeA = await actualA.GetAsync("ka");
            peerBeforeB = await actualB.GetAsync("kb");
            peerRepairSubmitted = true;
        });
        await maintainerB.DrainAsync();
        await maintainerB.DrainAsync();
        Assert.That(await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).GetAsync("ka"), Is.EqualTo(replacementA));
        Assert.That(await _fixture.Cluster.Client.GetGrain<ILattice>(treeB).GetAsync("kb"), Is.EqualTo(replacementB));
        Assert.That(peerRepairSubmitted, Is.True);
        Assert.That(peerBeforeA, Is.EqualTo(originalA));
        Assert.That(peerBeforeB, Is.EqualTo(originalB));
        Assert.That(await actualA.GetAsync("ka"), Is.EqualTo(originalA));
        Assert.That(await actualB.GetAsync("kb"), Is.EqualTo(originalB),
            "A participant's ordinary successor stays fenced while the mandatory repair lacks its peer.");

        var repairedSnapshotObserved = false;
        byte[]? beforeRepairA = null;
        byte[]? beforeRepairB = null;
        byte[]? afterRepairA = null;
        byte[]? afterRepairB = null;
        CrossTreeRegisterReadyHookFilter.BeforeNextRepair(viewA, async _ =>
        {
            beforeRepairA = await actualA.GetAsync("ka");
            beforeRepairB = await actualB.GetAsync("kb");
        });
        CrossTreeRegisterReadyHookFilter.AfterNextRepair(viewA, async _ =>
        {
            afterRepairA = await actualA.GetAsync("ka");
            afterRepairB = await actualB.GetAsync("kb");
            repairedSnapshotObserved = true;
            throw new TimeoutException("Injected lost committed joint repair response.");
        });
        var lostCompletion = false;
        for (var pass = 0; pass < 4 && !lostCompletion; pass++)
        {
            try
            {
                await maintainerA.DrainAsync();
            }
            catch (TimeoutException)
            {
                lostCompletion = true;
            }
        }
        Assert.That(lostCompletion, Is.True);
        Assert.That(repairedSnapshotObserved, Is.True);
        var storedA = await _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId).GetRepairReadinessAsync(viewA);
        var storedB = await _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId).GetRepairReadinessAsync(viewB);
        Assert.That(storedA, Is.Not.Null);
        Assert.That(storedB, Is.Not.Null);
        Assert.That(storedA!.Upserts.Single(pair => pair.Key == "ka").Value, Is.EqualTo(replacementA));
        Assert.That(storedB!.Upserts.Single(pair => pair.Key == "kb").Value, Is.EqualTo(replacementB));
        Assert.That(beforeRepairA, Is.EqualTo(originalA));
        Assert.That(beforeRepairB, Is.EqualTo(originalB));
        Assert.That(afterRepairA, Is.EqualTo(replacementA));
        Assert.That(afterRepairB, Is.EqualTo(replacementB),
            "Both replacement slices are public before either maintainer can resume ordinary work.");

        var successorA = Person(70, "post-capture-a");
        var successorB = Person(80, "post-capture-b");
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", successorA);
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeB).SetAsync("kb", successorB);
        byte[]? resumedA = null;
        byte[]? resumedB = null;
        CrossTreeRegisterReadyHookFilter.AfterNextRepair(viewA, async _ =>
        {
            resumedA = await actualA.GetAsync("ka");
            resumedB = await actualB.GetAsync("kb");
        });

        if (reactivate)
        {
            await maintainerA.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
            maintainerA = await MaintainerAsync(viewA);
        }

        // Completion retries must reattach to the frozen repair, and retirement
        // must durably precede the acknowledgement even if its response is lost.
        CrossTreeRegisterReadyHookFilter.LoseNextAcknowledgementResponse(viewA);
        var lostAck = false;
        for (var pass = 0; pass < 8 && !lostAck; pass++)
        {
            try
            {
                await maintainerA.DrainAsync();
            }
            catch (TimeoutException)
            {
                lostAck = true;
            }
        }

        Assert.That(lostAck, Is.True, "The actual post-checkpoint acknowledgement ran and its response was lost.");
        Assert.That(resumedA, Is.EqualTo(replacementA), "Restart resumes the immutable capture, not the changed source.");
        Assert.That(resumedB, Is.EqualTo(replacementB));
        storedA = await _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId).GetRepairReadinessAsync(viewA);
        Assert.That(storedA!.Upserts.Single(pair => pair.Key == "ka").Value, Is.EqualTo(replacementA));
        if (reactivate)
        {
            await maintainerA.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
            maintainerA = await MaintainerAsync(viewA);
        }

        for (var pass = 0; pass < 4; pass++)
        {
            await maintainerA.DrainAsync();
            await maintainerB.DrainAsync();
        }

        Assert.That(await actualA.GetAsync("ka"), Is.EqualTo(successorA));
        Assert.That(await actualB.GetAsync("kb"), Is.EqualTo(successorB));
        Assert.That(await maintainerA.GetLagAsync(), Is.Zero);
        Assert.That(await maintainerB.GetLagAsync(), Is.Zero);
    }
}
