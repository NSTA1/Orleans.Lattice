using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The area directory: keys validated when it is built, stops in directory
/// order, and every question it asks an area time-boxed and failing closed.
/// </summary>
[TestFixture]
public sealed class ExplorerAreaDirectoryTests
{
    private readonly ManualTimeProvider _time = new();

    [Test]
    public void Areas_are_held_in_directory_order_with_ties_broken_by_key()
    {
        var directory = Create(new FakeArea("schema", "Schema", 5), new FakeArea("apps", "Apps", 2), new FakeArea("access", "Access", 5));

        Assert.That(directory.Areas.Select(area => area.Key), Is.EqualTo(new[] { "apps", "access", "schema" }));
    }

    [Test]
    [TestCase("Data")]
    [TestCase("1data")]
    [TestCase("")]
    [TestCase("t")]
    [TestCase("not-found")]
    public void An_invalid_or_reserved_key_fails_loudly(string key)
    {
        Assert.That(() => Create(new FakeArea(key, "X")), Throws.InvalidOperationException);
    }

    [Test]
    public void A_duplicated_key_fails_loudly()
    {
        Assert.That(
            () => Create(new FakeArea("data", "Data"), new FakeArea("data", "Data again")),
            Throws.InvalidOperationException.With.Message.Contains("both declare the key 'data'"));
    }

    [Test]
    public void The_reserved_keys_are_the_tenant_segment_and_the_not_found_page()
    {
        Assert.That(ExplorerAreaDirectory.ReservedKeys, Is.EquivalentTo(new[] { "t", "not-found" }));
    }

    [Test]
    public void Find_returns_the_area_or_null()
    {
        var data = new FakeArea("data", "Data");
        var directory = Create(data);

        Assert.Multiple(() =>
        {
            Assert.That(directory.Find("data"), Is.SameAs(data));
            Assert.That(directory.Find("apps"), Is.Null);
            Assert.That(directory.Find(null), Is.Null);
        });
    }

    [Test]
    public async Task Entries_hold_visible_and_unavailable_areas_and_omit_hidden_ones()
    {
        var directory = Create(
            new FakeArea("data", "Data", 1),
            new FakeArea("backups", "Backups", 2) { Availability = _ => ValueTask.FromResult(AreaAvailability.Unavailable("Sign in to see backups.")) },
            new FakeArea("schema", "Schema", 3) { Availability = _ => ValueTask.FromResult(AreaAvailability.Hidden) });

        var entries = await directory.GetEntriesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.Area.Key), Is.EqualTo(new[] { "data", "backups" }));
            Assert.That(entries[0].IsVisible, Is.True);
            Assert.That(entries[1].IsVisible, Is.False);
            Assert.That(entries[1].Availability.Reason, Is.EqualTo("Sign in to see backups."));
        });
    }

    [Test]
    public async Task An_area_whose_probe_throws_is_hidden()
    {
        var directory = Create(new FakeArea("data", "Data") { Availability = _ => throw new InvalidOperationException("facade down") });

        Assert.That(await directory.GetEntriesAsync(), Is.Empty);
    }

    [Test]
    public async Task An_area_whose_probe_faults_asynchronously_is_hidden()
    {
        var directory = Create(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromException<AreaAvailability>(new TimeoutException()),
        });

        Assert.That(await directory.GetEntriesAsync(), Is.Empty);
    }

    [Test]
    public async Task An_area_whose_probe_cancels_itself_is_hidden()
    {
        var directory = Create(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromCanceled<AreaAvailability>(new CancellationToken(canceled: true)),
        });

        Assert.That(await directory.GetEntriesAsync(), Is.Empty);
    }

    [Test]
    public async Task An_area_that_does_not_answer_in_time_is_hidden_and_the_others_are_not_held_back()
    {
        var never = new TaskCompletionSource<AreaAvailability>();
        CancellationToken observed = default;
        var directory = Create(
            new FakeArea("data", "Data", 1),
            new FakeArea("slow", "Slow", 2)
            {
                Availability = token =>
                {
                    observed = token;
                    return new ValueTask<AreaAvailability>(never.Task);
                },
            });

        var pending = directory.GetEntriesAsync();
        Assert.That(pending.IsCompleted, Is.False, "the slow area is still being waited for");

        _time.Advance(new ExplorerChromeOptions().AvailabilityTimeout);
        var entries = await pending;

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.Area.Key), Is.EqualTo(new[] { "data" }));
            Assert.That(observed.IsCancellationRequested, Is.True, "the slow probe is told to stop");
        });
    }

    [Test]
    public void The_callers_cancellation_propagates()
    {
        var directory = Create(new FakeArea("data", "Data"));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(async () => await directory.GetEntriesAsync(cancelled.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task A_visible_area_carries_its_badge_and_a_failing_badge_is_omitted()
    {
        var askedUnavailable = false;
        var directory = Create(
            new FakeArea("data", "Data", 1) { Badge = _ => ValueTask.FromResult<string?>("1,204") },
            new FakeArea("apps", "Apps", 2) { Badge = _ => throw new InvalidOperationException() },
            new FakeArea("replication", "Replication", 3) { Badge = _ => ValueTask.FromResult<string?>("  ") },
            new FakeArea("backups", "Backups", 4)
            {
                Availability = _ => ValueTask.FromResult(AreaAvailability.Unavailable("No grant.")),
                Badge = _ =>
                {
                    askedUnavailable = true;
                    return ValueTask.FromResult<string?>("x");
                },
            });

        var entries = await directory.GetEntriesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.Badge), Is.EqualTo(new string?[] { "1,204", null, null, null }));
            Assert.That(askedUnavailable, Is.False, "an unavailable area is not asked for its badge");
        });
    }

    [Test]
    public async Task A_home_status_is_returned_and_fails_closed_to_none()
    {
        var never = new TaskCompletionSource<string?>();
        var directory = Create();

        var answered = await directory.GetHomeStatusAsync(new FakeArea("data", "Data") { HomeStatus = _ => ValueTask.FromResult<string?>("12 trees") });
        var blank = await directory.GetHomeStatusAsync(new FakeArea("data", "Data") { HomeStatus = _ => ValueTask.FromResult<string?>(" ") });
        var failed = await directory.GetHomeStatusAsync(new FakeArea("data", "Data") { HomeStatus = _ => throw new InvalidOperationException() });
        var pending = directory.GetHomeStatusAsync(new FakeArea("data", "Data") { HomeStatus = _ => new ValueTask<string?>(never.Task) });
        _time.Advance(new ExplorerChromeOptions().HomeStatusTimeout);

        Assert.Multiple(async () =>
        {
            Assert.That(answered, Is.EqualTo("12 trees"));
            Assert.That(blank, Is.Null);
            Assert.That(failed, Is.Null);
            Assert.That(await pending, Is.Null);
        });
    }

    [Test]
    public void Invalidate_raises_Changed()
    {
        var directory = Create();
        var raised = 0;
        directory.Changed += () => raised++;

        directory.Invalidate();

        Assert.That(raised, Is.EqualTo(1));
    }

    [Test]
    public void The_constructor_and_probes_reject_null()
    {
        var directory = Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => new ExplorerAreaDirectory(null!, new ExplorerChromeOptions(), _time), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerAreaDirectory([], null!, _time), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerAreaDirectory([], new ExplorerChromeOptions(), null!), Throws.ArgumentNullException);
            Assert.That(async () => await directory.GetAvailabilityAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await directory.GetHomeStatusAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await directory.GetDirectoryBadgeAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AreaAvailability_defaults_to_hidden_and_an_unavailable_answer_needs_a_reason()
    {
        Assert.Multiple(() =>
        {
            Assert.That(default(AreaAvailability).Kind, Is.EqualTo(AreaAvailabilityKind.Hidden));
            Assert.That(default(AreaAvailability).IsShown, Is.False);
            Assert.That(AreaAvailability.Visible.IsShown, Is.True);
            Assert.That(AreaAvailability.Unavailable("Why.").IsShown, Is.True);
            Assert.That(AreaAvailability.Visible.Reason, Is.Null);
            Assert.That(() => AreaAvailability.Unavailable(" "), Throws.ArgumentException);
        });
    }

    private ExplorerAreaDirectory Create(params IExplorerArea[] areas) =>
        new(areas, new ExplorerChromeOptions(), _time);
}
