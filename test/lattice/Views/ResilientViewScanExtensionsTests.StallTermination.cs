using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The termination half of the view wrappers' stall handling, and the
/// system-origin half of their scope re-assertion.
/// <para>
/// <c>ScanEntriesAsyncCore</c> carries its own copy of the reopen loop, so it
/// carries its own copy of the <see cref="ScanPageStalledException"/>
/// <i>termination</i> arm - the branch that records the outcome and rethrows
/// once the resume budget or the total ceiling is spent. Every existing stall
/// fixture drives that arm through <c>ScanKeysAsync</c> and only drives the
/// entries wrapper down its <i>resume</i> path, which is the shape the sibling
/// fixtures already warn about: a behaviour present at two sites is most likely
/// to be half-covered at the one nobody was looking at. A truncated entry scan
/// that silently looked complete would be a data-loss-shaped defect in an export,
/// so the arm that guarantees it cannot is worth owning directly.
/// </para>
/// <para>
/// The second group covers the <c>reassertSystemOrigin</c> arm of both wrappers.
/// The existing regression fixture drives the credential scope only, so the
/// system-origin conditional has never been evaluated true: a system-origin
/// caller whose scope is dropped on reopen resolves to a non-system subject and a
/// fail-closed gate truncates the scan, which is the same silent-truncation
/// failure the credential regression was raised for.
/// </para>
/// </summary>
public partial class ResilientViewScanExtensionsTests
{
    // ── ScanEntriesAsync stall termination ─────────────────────

    [Test]
    public void ScanEntriesAsync_exhausts_the_budget_when_a_stall_repeats_at_an_unchanged_continuation_token()
    {
        // The entries mirror of the keys fixture: a scan that stalls again at the
        // position it last stalled at spends the whole budget and then throws, so
        // a scan that cannot finish never looks finished.
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return callIndex++ == 0
                    ? StalledEntries(new[] { ("a", 1) }, stallAfter: 1)
                    : StalledEntries(Array.Empty<(string, int)>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanEntriesAsync()));

        Assert.That(
            calls,
            Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeAttempts),
            "every resume attempt is spent before the stall is rethrown");
    }

    [Test]
    public void ScanEntriesAsync_rethrows_the_stall_once_the_total_ceiling_is_exhausted()
    {
        // This source progresses one entry per stall, so the consecutive-futile
        // budget is replenished every time and the walk is bounded by the total
        // ceiling instead (issue 2539). That selects the other arm of the
        // termination outcome than the fixture above.
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        var next = 'a';
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledEntries(new[] { (next++.ToString(), 1) }, stallAfter: 1);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanEntriesAsync()));

        Assert.That(
            calls,
            Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeCeiling),
            "an endlessly dribbling entry walk must terminate at the total ceiling");
    }

    [Test]
    public void ScanEntriesAsync_does_not_resume_a_stall_when_max_attempts_is_zero()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledEntries(new[] { ("a", 1) }, stallAfter: 1);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanEntriesAsync(maxAttempts: 0)));

        Assert.That(calls, Is.EqualTo(1), "a zero reconnect budget leaves no stall budget to spend");
    }

    [Test]
    public void ScanEntriesAsync_never_ends_a_stalled_entry_scan_as_though_it_had_completed()
    {
        var view = Substitute.For<ILatticeView>();
        var next = 'a';
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => StalledEntries(new[] { (next++.ToString(), 1) }, stallAfter: 1));

        var yielded = new List<KeyValuePair<string, byte[]>>();
        var completedNormally = false;

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var e in view.ScanEntriesAsync())
            {
                yielded.Add(e);
            }

            completedNormally = true;
        });

        Assert.That(completedNormally, Is.False, "a truncated entry scan must not look complete");
        Assert.That(yielded, Is.Not.Empty, "the entries delivered before the stall are still delivered");
    }

    [Test]
    public void ScanEntriesAsync_still_exhausts_the_budget_when_progress_stops()
    {
        // The first page stalls at the origin, so a resume is genuinely spent
        // before any progress happens - which is what makes the call count
        // discriminate the budget reset rather than merely tolerate it.
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = calls++;
                return index == 1
                    ? StalledEntries(new[] { ("a", 1) }, stallAfter: 1)
                    : StalledEntries(Array.Empty<(string, int)>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanEntriesAsync()));

        Assert.That(
            calls,
            Is.EqualTo(2 + LatticeExtensions.DefaultScanStallResumeAttempts),
            "progress replenishes the budget once; it does not make the walk immortal");
    }

    [Test]
    public void ScanEntriesAsync_negative_maxAttempts_is_clamped_to_zero()
    {
        // The entries wrapper carries its own copy of the budget clamp, and the
        // keys wrapper's equivalent fixture never reached it.
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
            await CollectAsync(view.ScanEntriesAsync(maxAttempts: -5)));

        Assert.That(callIndex, Is.EqualTo(1), "a negative budget clamps to zero - no reconnects attempted");
    }

    [Test]
    public void ScanKeysAsync_reports_the_scan_origin_when_a_stall_terminates_before_any_key_is_yielded()
    {
        // Every existing termination fixture has yielded at least one key, so the
        // terminal record has always carried a resume position. A stall at the
        // origin with no budget reports the caller's own lower bound instead, and
        // says it has no continuation - the arm a truncated-from-the-start export
        // depends on being reported honestly.
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        StubKeys(view, _ =>
        {
            calls++;
            return StalledKeys(Array.Empty<string>(), stallAfter: 0);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanKeysAsync("start-here", maxAttempts: 0)));

        Assert.That(calls, Is.EqualTo(1), "a zero budget leaves nothing to resume with");
    }

    [Test]
    public void ScanEntriesAsync_reports_the_scan_origin_when_a_stall_terminates_before_any_entry_is_yielded()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledEntries(Array.Empty<(string, int)>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanEntriesAsync("start-here", maxAttempts: 0)));

        Assert.That(calls, Is.EqualTo(1), "a zero budget leaves nothing to resume with");
    }

    // ── System-origin re-assertion across reopen ───────────────

    [Test]
    public async Task ScanKeysAsync_reasserts_caller_system_origin_across_reopen()
    {
        // The system-origin twin of the credential regression: Orleans resets the
        // caller-established RequestContext in the iterator's execution flow after
        // the first physical segment, so a scan opened under a system-origin scope
        // must re-assert it on every reopen or the resumed segment resolves as a
        // non-system subject and a fail-closed gate silently truncates the scan.
        // The reset is simulated by clearing the ambient marker inside the
        // aborting first segment, exactly as the credential fixture does.
        var observed = new List<bool>();
        var callIndex = 0;
        var view = Substitute.For<ILatticeView>();
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed.Add(LatticeAccessGateContext.IsSystemOrigin);
                if (callIndex++ == 0)
                {
                    ClearSystemOriginMarker();
                    return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
                }

                return ScriptedKeys(new[] { "c" }, abortAfter: int.MaxValue);
            });

        List<string> keys;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            keys = await CollectAsync(view.ScanKeysAsync());
        }

        Assert.Multiple(() =>
        {
            Assert.That(callIndex, Is.EqualTo(2), "the scan must reopen once after the abort");
            Assert.That(observed[0], Is.True, "the first segment carries the caller's system origin");
            Assert.That(observed[1], Is.True,
                "the reopened segment must re-assert the system origin (non-system before the fix)");
            Assert.That(keys, Is.EqualTo(new[] { "c" }));
        });
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the caller's scope still unwinds cleanly");
    }

    [Test]
    public async Task ScanEntriesAsync_reasserts_caller_system_origin_across_reopen()
    {
        var observed = new List<bool>();
        var callIndex = 0;
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed.Add(LatticeAccessGateContext.IsSystemOrigin);
                if (callIndex++ == 0)
                {
                    ClearSystemOriginMarker();
                    return ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0);
                }

                return ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        List<KeyValuePair<string, byte[]>> entries;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            entries = await CollectAsync(view.ScanEntriesAsync());
        }

        Assert.Multiple(() =>
        {
            Assert.That(callIndex, Is.EqualTo(2), "the scan must reopen once after the abort");
            Assert.That(observed[0], Is.True, "the first segment carries the caller's system origin");
            Assert.That(observed[1], Is.True,
                "the reopened segment must re-assert the system origin (non-system before the fix)");
            Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "c" }));
        });
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the caller's scope still unwinds cleanly");
    }

    /// <summary>
    /// Drops the ambient system-origin marker the way Orleans drops a
    /// caller-established <see cref="RequestContext"/> entry between physical
    /// segments of an iterator.
    /// </summary>
    private static void ClearSystemOriginMarker() =>
        RequestContext.Remove(LatticeEventConstants.AccessGateSystemOriginRequestContextKey);

    // ── Reconnect backoff ──────────────────────────────────────

    [Test]
    public async Task ScanEntriesAsync_applies_the_linear_reconnect_backoff_past_the_first_attempt()
    {
        // Three consecutive aborts drive the backoff past its immediate-first-retry
        // arm and into the 10ms-per-attempt ramp, which is the only path through
        // ComputeReconnectDelayMs that actually awaits.
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => callIndex++ < 3
                ? ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0)
                : ScriptedEntries(new[] { ("a", 1) }, abortAfter: int.MaxValue));

        var entries = await CollectAsync(view.ScanEntriesAsync());

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a" }));
        Assert.That(callIndex, Is.EqualTo(4), "three aborts are absorbed before the scan completes");
    }
}
