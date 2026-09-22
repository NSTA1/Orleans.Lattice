using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable conformance suite proving a WAL storage provider treats its
/// highest-offset report as a <b>monotonic high-water mark</b> rather than a
/// maximum over live entries.
/// <para>
/// The WAL grain recovers its allocation counter on activation as
/// <c>_nextOffset = GetHighestOffsetAsync() + 1</c>. A provider that answers
/// from its live entries therefore reports <c>-1</c> once a trim removes them
/// all, and the grain restarts allocation at offset <c>0</c> while the shard's
/// durable trim floor still sits far above it. Every entry appended after that
/// is at or below the floor: it commits, acknowledges, and reads back normally
/// for the life of the process, and the next recovery classifies it as
/// already-trimmed and discards it. On a durable provider that is silent data
/// loss; on a volatile one it is offset reuse against cursors that have already
/// consumed those offsets.
/// </para>
/// <para>
/// This suite exists because the trap is not obvious from the interface. Of the
/// three providers in this repository when issue #3366 was diagnosed, two had
/// independently fallen into it and one had not, and the contract wording
/// ("the highest offset currently persisted") actively invited the wrong
/// reading. Pinning the property once here means a new provider inherits the
/// guard by construction instead of repeating the diagnosis.
/// </para>
/// <para>
/// The base is <see langword="abstract"/> so it is never discovered on its own;
/// the inherited <c>[Test]</c>s run through the concrete subclass in each
/// provider's test assembly.
/// </para>
/// </summary>
public abstract class WalOffsetAllocationContractTestsBase
{
    /// <summary>
    /// Creates a probe over a fresh, empty shard. Each test disposes its own
    /// probe, so implementations may allocate per-test scratch storage.
    /// </summary>
    protected abstract Task<IWalOffsetAllocationProbe> CreateProbeAsync();

    /// <summary>
    /// Models the WAL grain's activation arithmetic, which is the only reason
    /// the highest-offset report exists.
    /// </summary>
    private static async Task<long> NextOffsetAsync(IWalOffsetAllocationProbe probe) =>
        await probe.GetHighestOffsetAsync(CancellationToken.None).ConfigureAwait(false) + 1;

    [Test]
    public async Task Highest_offset_is_minus_one_before_any_append()
    {
        await using var probe = await CreateProbeAsync();

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(-1L),
            "The never-written sentinel is -1, so the first allocated offset is 0.");
    }

    [Test]
    public async Task Highest_offset_reports_the_live_tail()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L));
    }

    [Test]
    public async Task Highest_offset_does_not_regress_when_every_entry_is_trimmed()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);

        await probe.TrimAsync(2L, CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L),
            "A trim must never lower the high-water mark. Reporting -1 here "
            + "restarts allocation at 0, beneath the trim floor (issue #3366).");
    }

    [Test]
    public async Task Highest_offset_is_unchanged_by_a_partial_trim()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);

        await probe.TrimAsync(0L, CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L));
    }

    [Test]
    public async Task Lowest_offset_is_minus_one_when_every_entry_is_trimmed()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L }, CancellationToken.None);

        await probe.TrimAsync(1L, CancellationToken.None);

        Assert.That(
            await probe.GetLowestOffsetAsync(CancellationToken.None),
            Is.EqualTo(-1L),
            "The lowest LIVE offset is the instrument that reports emptiness; "
            + "the highest offset deliberately does not.");
    }

    [Test]
    public async Task Highest_offset_survives_a_reopen_after_a_full_trim()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);
        await probe.TrimAsync(2L, CancellationToken.None);

        await probe.ReopenAsync(CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L),
            "A durable provider must restore its trim floor and answer from it; "
            + "otherwise the restart is exactly when allocation regresses.");
    }

    /// <summary>
    /// The end-to-end property the other tests exist to protect: an entry
    /// appended after a full trim must still be there after a restart.
    /// </summary>
    [Test]
    public async Task An_entry_appended_after_a_full_trim_survives_a_reopen()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);
        await probe.TrimAsync(2L, CancellationToken.None);
        await probe.ReopenAsync(CancellationToken.None);

        var appendedAt = await NextOffsetAsync(probe);
        await probe.AppendAsync(new[] { appendedAt }, CancellationToken.None);

        // The write is readable here whether or not the provider is correct -
        // which is what made this defect present as unexplained loss rather
        // than as a failed write.
        Assert.That(
            await probe.ReadLiveOffsetsAsync(CancellationToken.None),
            Is.EqualTo(new[] { appendedAt }),
            "The append must be readable before the reopen.");

        await probe.ReopenAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(
                appendedAt,
                Is.EqualTo(3L),
                "Allocation must resume strictly above the trim floor.");
            Assert.That(
                await probe.ReadLiveOffsetsAsync(CancellationToken.None),
                Is.EqualTo(new[] { 3L }),
                "The acknowledged append must survive. An empty result is the "
                + "issue #3366 data loss: the entry was born at or below the "
                + "trim floor and silently discarded by recovery.");
        });
    }

    [Test]
    public async Task A_trim_recorded_on_an_empty_shard_does_not_lower_the_high_water_mark()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(new[] { 0L, 1L, 2L }, CancellationToken.None);

        await probe.TrimAsync(2L, CancellationToken.None);
        await probe.TrimAsync(2L, CancellationToken.None);

        Assert.That(
            await probe.GetHighestOffsetAsync(CancellationToken.None),
            Is.EqualTo(2L),
            "Trimming is idempotent and must stay non-lowering when repeated.");
    }

    // Deliberately NOT asserted here: that a trim recorded against a shard which
    // never held an entry raises the high-water mark on its own.
    //
    // FileWalShard does do this - it persists a trim record unconditionally, so
    // the floor binds allocation even with nothing appended, and
    // FileWalShardRecoveryTests pins that behaviour for the file provider.
    // AzureTableWalStorageProvider, the reference implementation, does not:
    // TrimAsync only ever queries `RowKey lt 'TAIL'` and never writes the TAIL
    // row ("TAIL is never moved back by trim"), so a trim against a shard with
    // no manifest rows leaves nothing behind and GetHighestOffsetAsync still
    // 404s to -1. InMemoryWalStorageProvider matches the reference.
    //
    // Requiring it here would fail the one provider that was already correct,
    // which would be the suite disagreeing with the contract rather than the
    // provider disagreeing with the suite. The property that actually protects
    // allocation is the one above - never regress below an offset that was
    // assigned - and all three providers hold it.

}
