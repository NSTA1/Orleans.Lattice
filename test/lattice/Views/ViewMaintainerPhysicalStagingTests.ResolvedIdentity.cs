namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [Test]
    public async Task Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids()
        => await AssertResolvedIdentityAsync(restoreOldResolved: false);

    [Test]
    public void Drain_resolved_identity_detector_rejects_restored_old_log_cache_control()
    {
        var failure = Assert.ThrowsAsync<AssertionException>(async () =>
            await AssertResolvedIdentityAsync(restoreOldResolved: true));
        Assert.That(failure!.Message, Does.Contain("new-complete"));
    }

    private static async Task AssertResolvedIdentityAsync(bool restoreOldResolved)
    {
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-complete", 11));
        h.Append(Old, 8, Terminal(Old));
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key), Is.EqualTo(new[] { "old-complete" }));
        h.Append(Old, 9, Prepare(Old, "old-duplicate", 12));
        h.Append(Old, 10, Terminal(Old));
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key), Is.EqualTo(new[] { "old-complete" }),
            "resolved duplicate suppression must remain intact within the same physical log");
        var oldResolved = ((HashSet<Guid>)Field("_resolved").GetValue(h.Grain)!).ToArray();
        h.Alias = New;
        await h.Grain.NotifySourceIdentityChangedAsync(New);
        await h.Grain.DrainAsync();
        if (restoreOldResolved)
        {
            ((HashSet<Guid>)Field("_resolved").GetValue(h.Grain)!).UnionWith(oldResolved);
        }
        h.Append(New, 2, Prepare(New, "new-complete", 21));
        h.Append(New, 3, Terminal(New));

        await h.Grain.DrainAsync();

        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key),
            Is.EqualTo(new[] { "old-complete", "new-complete" }),
            "resolved ids belong to one physical log; its alias successor can reuse the id without losing the new batch");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(3));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Drain_failed_candidate_publication_then_original_log_retry_keeps_resolved_ownership(bool committed)
    {
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-complete", 11));
        h.Append(Old, 8, Terminal(Old));
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.Count, Is.EqualTo(1));
        h.Alias = New;
        var capture = h.State.OnWriteState;
        if (committed)
        {
            h.State.OnWriteState = s =>
            {
                capture!(s);
                throw new TimeoutException("candidate committed but response lost before any new-log tail");
            };
        }
        else
        {
            h.State.ThrowOnWrite = new TimeoutException("candidate publication did not commit");
        }
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());
        h.State.OnWriteState = capture;
        h.Alias = Old;
        await h.Grain.NotifySourceIdentityChangedAsync(Old);
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Old));
        Assert.That(h.Reads.All(x => x.Tree == Old), Is.True,
            "candidate capture and uncertain publication never entered the candidate's tail");
        h.Append(Old, 9, Prepare(Old, "old-duplicate", 12));
        h.Append(Old, 10, Terminal(Old));

        await h.Grain.DrainAsync();

        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key), Is.EqualTo(new[] { "old-complete" }),
            "candidate admission or a failed response must not evict resolved ownership of the original log");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(10));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Drain_same_physical_log_rebuild_keeps_resolved_duplicate_suppression(bool rebuild)
    {
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-complete", 11));
        h.Append(Old, 8, Terminal(Old));
        await h.Grain.DrainAsync();
        if (rebuild) await h.Grain.RebuildAsync();
        h.Append(Old, 9, Prepare(Old, "old-duplicate", 12));
        h.Append(Old, 10, Terminal(Old));

        await h.Grain.DrainAsync();

        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key),
            Is.EqualTo(new[] { "old-complete" }),
            "same-log rebuild does not change ownership or permit an already resolved transaction to publish twice");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(10));
    }
}
