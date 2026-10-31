using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// Issue #5420: a probe the cluster did not answer is not an answer. The area
/// stays on the spine - as its last shown answer marked stale, or as not checked -
/// is checked again on a bounded backoff and on demand, and never admits a caller,
/// while a definite answer (a denial, an unserved facade) still hides it.
/// </summary>
public sealed partial class ExplorerAreaDirectoryTests
{
    private static readonly ExplorerChromeOptions TwoRechecks = new()
    {
        RecheckDelays = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)],
    };

    private static IEnumerable<TestCaseData> TransientFaults()
    {
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Internal, "activation lost"))).SetName("{m}(Internal)");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Unavailable, "down"))).SetName("{m}(Unavailable)");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow"))).SetName("{m}(DeadlineExceeded)");
        yield return new TestCaseData(new ShellTransportException("x", isTransient: false, new RpcException(new Status(StatusCode.Internal, "x")))).SetName("{m}(transport wrapping Internal)");
        yield return new TestCaseData(new LatticeStateApiException("The state-API call failed (Internal).", new RpcException(new Status(StatusCode.Internal, "x"))) { IsTransient = true }).SetName("{m}(state API Internal)");
        yield return new TestCaseData(new HttpRequestException("connection reset")).SetName("{m}(HTTP transport)");
        yield return new TestCaseData(new TimeoutException()).SetName("{m}(timeout)");
    }

    private static IEnumerable<TestCaseData> DefiniteFaults()
    {
        yield return new TestCaseData(new LatticeAuthorizationDeniedException("denied")).SetName("{m}(gate denial)");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.PermissionDenied, "no"))).SetName("{m}(PermissionDenied)");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Unauthenticated, "no"))).SetName("{m}(Unauthenticated)");
        yield return new TestCaseData(new LatticeStateApiException("denied", new RpcException(new Status(StatusCode.Internal, "x"))) { IsPermissionDenied = true }).SetName("{m}(denial over Internal)");
        yield return new TestCaseData(new NotSupportedException()).SetName("{m}(not served)");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Unimplemented, "no"))).SetName("{m}(Unimplemented)");
        yield return new TestCaseData(new InvalidOperationException("not configured")).SetName("{m}(unrecognised)");
    }

    [TestCaseSource(nameof(TransientFaults))]
    public async Task A_transient_fault_keeps_the_area_listed_as_not_checked(Exception fault)
    {
        var directory = Create(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromException<AreaAvailability>(fault) });

        var entries = await directory.GetEntriesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.Area.Key), Is.EqualTo(new[] { "access" }), "the area is not dropped");
            Assert.That(entries[0].IsUnchecked, Is.True);
            Assert.That(entries[0].IsVisible, Is.False, "a fault never admits the caller");
            Assert.That(entries[0].NeedsRecheck, Is.True);
            Assert.That(entries[0].Availability.Reason, Is.EqualTo(AreaAvailability.UncheckedReason));
            Assert.That(directory.IsRecheckScheduled, Is.True);
        });
    }

    [TestCaseSource(nameof(DefiniteFaults))]
    public async Task A_definite_fault_still_hides_the_area(Exception fault)
    {
        var directory = Create(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromException<AreaAvailability>(fault) });

        Assert.Multiple(async () =>
        {
            Assert.That(await directory.GetEntriesAsync(), Is.Empty);
            Assert.That(directory.IsRecheckScheduled, Is.False, "a definite answer is not checked again");
        });
    }

    [Test]
    public async Task An_area_that_reports_it_could_not_check_is_shown_not_checked()
    {
        var directory = Create(new FakeArea("replication", "Replication") { Availability = _ => ValueTask.FromResult(AreaAvailability.Unchecked) });

        var entries = await directory.GetEntriesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Single().IsUnchecked, Is.True);
            Assert.That(directory.IsRecheckScheduled, Is.True);
        });
    }

    [Test]
    public async Task The_last_shown_answer_stands_in_marked_stale_with_its_badge()
    {
        Exception? fault = null;
        var area = new FakeArea("access", "Access")
        {
            Availability = _ => fault is null ? ValueTask.FromResult(AreaAvailability.Visible) : ValueTask.FromException<AreaAvailability>(fault),
            Badge = _ => ValueTask.FromResult<string?>("12"),
        };
        var directory = Create(area);

        var before = (await directory.GetEntriesAsync()).Single();
        fault = new RpcException(new Status(StatusCode.Internal, "activation lost"));
        var during = (await directory.GetEntriesAsync()).Single();
        var asked = await directory.GetAvailabilityAsync(area);
        fault = null;
        var after = (await directory.GetEntriesAsync()).Single();

        Assert.Multiple(() =>
        {
            Assert.That(before.IsStale, Is.False);
            Assert.That(during.IsVisible, Is.True, "it was visible to this caller a moment ago");
            Assert.That(during.IsStale, Is.True);
            Assert.That(during.Badge, Is.EqualTo("12"));
            Assert.That(asked, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(after.IsStale, Is.False);
            Assert.That(directory.IsRecheckScheduled, Is.False, "an answer settles the recheck");
        });
    }

    [Test]
    public async Task A_hidden_answer_stays_hidden_when_the_next_check_meets_no_answer()
    {
        Exception? fault = null;
        var area = new FakeArea("access", "Access")
        {
            Availability = _ => fault is null ? ValueTask.FromResult(AreaAvailability.Hidden) : ValueTask.FromException<AreaAvailability>(fault),
        };
        var directory = Create(area);

        var refused = await directory.GetEntriesAsync();
        fault = new RpcException(new Status(StatusCode.Unavailable, "down"));
        var during = await directory.GetEntriesAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(refused, Is.Empty);
            Assert.That(during, Is.Empty, "a fault never reveals an area this caller was refused");
            Assert.That(await directory.GetAvailabilityAsync(area), Is.EqualTo(AreaAvailability.Hidden));
        });
    }

    [Test]
    public async Task What_was_read_for_one_caller_is_never_shown_to_the_next()
    {
        var auth = new FakeAuthSession();
        auth.SignIn("olivia");
        using var caller = new ShellCaller(auth);
        Exception? fault = null;
        var area = new FakeArea("access", "Access")
        {
            Availability = _ => fault is null ? ValueTask.FromResult(AreaAvailability.Visible) : ValueTask.FromException<AreaAvailability>(fault),
        };
        var directory = new ExplorerAreaDirectory([area], new ExplorerChromeOptions(), _time, caller: caller);

        await directory.GetEntriesAsync();
        auth.SignIn("nina");
        fault = new RpcException(new Status(StatusCode.Internal, "activation lost"));
        var entry = (await directory.GetEntriesAsync()).Single();

        Assert.Multiple(() =>
        {
            Assert.That(entry.IsUnchecked, Is.True, "olivia's answer does not stand in for nina");
            Assert.That(entry.IsStale, Is.False);
        });
    }

    [Test]
    public async Task Rechecks_back_off_stop_when_spent_and_a_manual_retry_restarts_them()
    {
        Exception? fault = new RpcException(new Status(StatusCode.Unavailable, "down"));
        var area = new FakeArea("access", "Access")
        {
            Availability = _ => fault is null ? ValueTask.FromResult(AreaAvailability.Visible) : ValueTask.FromException<AreaAvailability>(fault),
        };
        var directory = new ExplorerAreaDirectory([area], TwoRechecks, _time);
        var changed = 0;
        directory.Changed += () => Interlocked.Increment(ref changed);

        await directory.GetEntriesAsync();
        _time.Advance(TimeSpan.FromMilliseconds(999));
        var beforeFirstDelay = changed;
        _time.Advance(TimeSpan.FromMilliseconds(1));
        var afterFirstDelay = changed;

        await directory.GetEntriesAsync();
        _time.Advance(TimeSpan.FromSeconds(2));
        var afterSecondDelay = changed;

        await directory.GetEntriesAsync();
        var scheduledWhenSpent = directory.IsRecheckScheduled;
        _time.Advance(TimeSpan.FromMinutes(5));
        var afterSpent = changed;

        directory.Recheck();
        var afterManual = changed;
        await directory.GetEntriesAsync();
        var scheduledAfterManual = directory.IsRecheckScheduled;

        fault = null;
        await directory.GetEntriesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(beforeFirstDelay, Is.EqualTo(0));
            Assert.That(afterFirstDelay, Is.EqualTo(1), "the first recheck falls due after the first delay");
            Assert.That(afterSecondDelay, Is.EqualTo(2), "the second after the second");
            Assert.That(scheduledWhenSpent, Is.False, "the rechecks are bounded");
            Assert.That(afterSpent, Is.EqualTo(2));
            Assert.That(afterManual, Is.EqualTo(3), "a manual retry checks again at once");
            Assert.That(scheduledAfterManual, Is.True, "and restarts the backoff");
            Assert.That(directory.IsRecheckScheduled, Is.False, "an answer settles it");
        });
    }

    [Test]
    public async Task A_home_status_that_meets_no_answer_shows_the_last_line_marked_stale()
    {
        Exception? fault = null;
        var area = new FakeArea("backups", "Backups")
        {
            HomeStatus = _ => fault is null ? ValueTask.FromResult<string?>("3 backups") : ValueTask.FromException<string?>(fault),
        };
        var directory = Create(area);

        await directory.GetHomeStatusAsync(area);
        fault = new ShellTransportException("The backup control-API request failed.", isTransient: false, new RpcException(new Status(StatusCode.Internal, "x")));
        var stale = await directory.GetHomeStatusAsync(area);
        var scheduled = directory.IsRecheckScheduled;
        fault = null;
        var fresh = await directory.GetHomeStatusAsync(area);

        Assert.Multiple(() =>
        {
            Assert.That(stale, Is.EqualTo(new ExplorerAreaStatus { Text = "3 backups", IsStale = true }));
            Assert.That(stale!.NeedsRecheck, Is.True);
            Assert.That(scheduled, Is.True);
            Assert.That(fresh, Is.EqualTo(new ExplorerAreaStatus { Text = "3 backups" }));
            Assert.That(directory.IsRecheckScheduled, Is.False);
        });
    }

    [Test]
    public async Task Disposing_the_directory_cancels_a_waiting_recheck()
    {
        var directory = new ExplorerAreaDirectory(
            [new FakeArea("access", "Access") { Availability = _ => ValueTask.FromResult(AreaAvailability.Unchecked) }],
            TwoRechecks,
            _time);
        var changed = 0;
        directory.Changed += () => changed++;

        await directory.GetEntriesAsync();
        directory.Dispose();
        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.EqualTo(0));
            Assert.That(directory.IsRecheckScheduled, Is.False);
        });
    }
}
