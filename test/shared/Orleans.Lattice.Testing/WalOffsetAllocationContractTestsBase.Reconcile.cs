using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable conformance suite for a WAL storage provider's <c>ReconcileAsync</c>:
/// reconcile never lowers the high-water mark and never discards an acknowledged
/// append, <b>whether or not appends to the shard are still settling</b>.
/// <para>
/// The WAL grain calls <c>ReconcileAsync</c> in two places, and both sit directly
/// on the allocation path: on activation, and in the post-failure resync after a
/// flush fails, each immediately before
/// <c>_nextOffset = GetHighestOffsetAsync() + 1</c>. The activation call is
/// quiescent. The resync call is <b>not</b>: the grain's own flush chain has
/// drained, but a provider that returns from an append before finishing it (the
/// Azure Tables provider's pipelined phase-2 commit is the case in point) may
/// still be completing appends the grain has already acknowledged to its callers.
/// </para>
/// <para>
/// Issue #3348 found the reference provider breaking the contract exactly there:
/// its reconcile assumed it had the shard to itself, so under load it treated
/// live batches as orphans, rewrote its tail below offsets already handed out,
/// and wedged the shard. Pinning the provider-agnostic rule here means a new
/// provider inherits the guard rather than rediscovering it under load.
/// </para>
/// <para>
/// Every enrolled provider inherits these tests. A provider whose probe cannot
/// yet drive reconcile stubs <see cref="IWalOffsetAllocationProbe.ReconcileAsync"/>
/// and <see cref="IWalOffsetAllocationProbe.AppendAcknowledgedAsync"/> with
/// <c>Assert.Ignore</c> and a TODO, so the gap shows in every test run rather
/// than disappearing.
/// </para>
/// </summary>
public abstract partial class WalOffsetAllocationContractTestsBase
{
    private const int SettlingBatchCount = 8;
    private const int SettlingBatchSize = 4;

    [Test]
    public async Task Reconcile_on_an_empty_shard_leaves_it_empty()
    {
        await using var probe = await CreateProbeAsync();

        await probe.ReconcileAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await probe.GetHighestOffsetAsync(CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await probe.ReadLiveOffsetsAsync(CancellationToken.None), Is.Empty);
        });
    }

    [Test]
    public async Task Reconcile_on_a_clean_shard_is_idempotent()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);

        await probe.ReconcileAsync(CancellationToken.None);
        await probe.ReconcileAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await probe.GetHighestOffsetAsync(CancellationToken.None), Is.EqualTo(2L));
            Assert.That(
                await probe.ReadLiveOffsetsAsync(CancellationToken.None),
                Is.EqualTo(new[] { 0L, 1L, 2L }),
                "A committed batch is not an orphan; reconcile must never remove it.");
        });
    }

    [Test]
    public async Task Reconcile_does_not_lower_the_high_water_mark_after_a_full_trim()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);
        await probe.TrimAsync(2L, CancellationToken.None);
        await probe.ReopenAsync(CancellationToken.None);

        await probe.ReconcileAsync(CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L),
            "Reconcile runs immediately before the grain derives its next offset, "
            + "so a reconcile that lowered the tail would restart allocation beneath "
            + "the trim floor exactly as issue #3366 did.");
    }

    /// <summary>
    /// Models the post-failure resync: several appends have been acknowledged
    /// back to back, the provider may still be completing them, and the grain
    /// reconciles and resumes allocation without waiting for anything else.
    /// </summary>
    [Test]
    public async Task Reconcile_while_acknowledged_appends_are_settling_keeps_every_one()
    {
        await using var probe = await CreateProbeAsync();
        var acknowledged = new List<long>();
        for (var batch = 0; batch < SettlingBatchCount; batch++)
        {
            var offsets = Batch(acknowledged.Count, SettlingBatchSize);
            await probe.AppendAcknowledgedAsync(offsets, CancellationToken.None);
            acknowledged.AddRange(offsets);
        }

        var resumedAt = await ResyncNextOffsetAsync(probe);

        Assert.That(
            resumedAt,
            Is.EqualTo((long)acknowledged.Count),
            "Allocation must resume one above the last acknowledged offset. A lower "
            + "value re-issues an offset a caller has already been told is durable.");

        await AssertResumedAppendAndRestartKeepEverythingAsync(probe, acknowledged, resumedAt);
    }

    /// <summary>
    /// The sharper form of the case above: reconcile is entered while an append
    /// has not yet even returned. A provider must either wait the append out or
    /// treat whatever of it has landed as live - never as an orphan to discard.
    /// </summary>
    [Test]
    public async Task Reconcile_entered_while_an_append_is_in_motion_keeps_that_append()
    {
        await using var probe = await CreateProbeAsync();
        var acknowledged = new List<long>();
        for (var batch = 0; batch < SettlingBatchCount - 1; batch++)
        {
            var offsets = Batch(acknowledged.Count, SettlingBatchSize);
            await probe.AppendAcknowledgedAsync(offsets, CancellationToken.None);
            acknowledged.AddRange(offsets);
        }

        var last = Batch(acknowledged.Count, SettlingBatchSize);
        var inMotion = probe.AppendAcknowledgedAsync(last, CancellationToken.None);
        var reconcile = probe.ReconcileAsync(CancellationToken.None);
        await Task.WhenAll(inMotion, reconcile);
        acknowledged.AddRange(last);

        var resumedAt = await ResyncNextOffsetAsync(probe);

        Assert.That(resumedAt, Is.EqualTo((long)acknowledged.Count));

        await AssertResumedAppendAndRestartKeepEverythingAsync(probe, acknowledged, resumedAt);
    }

    /// <summary>
    /// Models the WAL grain's resync arithmetic: reconcile the shard, then resume
    /// allocation one above the reported tail.
    /// </summary>
    private static async Task<long> ResyncNextOffsetAsync(IWalOffsetAllocationProbe probe)
    {
        await probe.ReconcileAsync(CancellationToken.None).ConfigureAwait(false);
        return await probe.GetHighestOffsetAsync(CancellationToken.None).ConfigureAwait(false) + 1;
    }

    private static long[] Batch(int start, int size) =>
        Enumerable.Range(start, size).Select(static offset => (long)offset).ToArray();

    /// <summary>
    /// Resumes allocation the way the grain does after a resync, then checks that
    /// nothing acknowledged was lost either before or across a restart.
    /// <see cref="IWalOffsetAllocationProbe.AppendAsync"/> crosses the provider's
    /// durability barrier, so the reads below see every earlier append settled.
    /// </summary>
    private static async Task AssertResumedAppendAndRestartKeepEverythingAsync(
        IWalOffsetAllocationProbe probe,
        List<long> acknowledged,
        long resumedAt)
    {
        var resumed = Batch((int)resumedAt, SettlingBatchSize);
        await probe.AppendAsync(resumed, CancellationToken.None);
        var expected = acknowledged.Concat(resumed).ToArray();

        Assert.That(
            await probe.ReadLiveOffsetsAsync(CancellationToken.None),
            Is.EqualTo(expected),
            "Every acknowledged append must be readable, exactly once, after the resync.");

        await probe.ReopenAsync(CancellationToken.None);
        await probe.ReconcileAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(
                await probe.ReadLiveOffsetsAsync(CancellationToken.None),
                Is.EqualTo(expected),
                "A restart's reconcile must not discard anything the resync kept.");
            Assert.That(
                await probe.GetHighestOffsetAsync(CancellationToken.None),
                Is.EqualTo(expected[^1]));
        });
    }
}
