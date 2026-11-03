using Grpc.Core;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The listing wait's contract (#5744): it returns once the listing holds every expected row,
/// keeps waiting past the stall window while the listed count rises, fails strictly when reads
/// succeed but the count stops rising, tells a listing it could never read apart from one that
/// read short, and cuts a read that hangs at the wait's own bound.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class ListingWaitSelfTests
{
    private static readonly TimeSpan Stall = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan Ceiling = TimeSpan.FromMinutes(1);

    /// <summary>A listing that already holds every row returns complete after one read.</summary>
    [Test]
    public async Task A_complete_listing_returns_at_once()
    {
        var result = await ListingWait.UntilListedAsync("the self-test listing", _ => Rows(3), rows => rows.Count, 3, Stall, Ceiling, Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Complete, Is.True);
            Assert.That(result.Rows, Has.Count.EqualTo(3));
            Assert.That(result.SuccessfulReads, Is.EqualTo(1));
            Assert.That(result.FailedReads, Is.Zero);
        });
    }

    /// <summary>A listed count that keeps rising outlasts the stall window until it reaches the target.</summary>
    [Test]
    public async Task A_rising_count_outlasts_the_stall_window()
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var listed = 0;

        var result = await ListingWait.UntilListedAsync(
            "the self-test listing",
            _ => Rows(clock.Elapsed > Stall * 2 ? 10 : Math.Min(9, ++listed)),
            rows => rows.Count,
            10,
            Stall,
            Ceiling,
            Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Complete, Is.True);
            Assert.That(clock.Elapsed, Is.GreaterThan(Stall * 2));
        });
    }

    /// <summary>Reads that succeed with a count that stops rising fail strictly, naming the count's trajectory.</summary>
    [Test]
    public async Task A_short_listing_that_stops_rising_is_incomplete_and_names_its_trajectory()
    {
        var result = await ListingWait.UntilListedAsync("the self-test listing", _ => Rows(7), rows => rows.Count, 9, Stall, Ceiling, Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Complete, Is.False);
            Assert.That(result.Unreadable, Is.False, "a listing that read short was read");
            Assert.That(result.SuccessfulReads, Is.GreaterThan(0));
            Assert.That(result.Rows, Has.Count.EqualTo(7));
            Assert.That(result.Failure, Does.Contain("made no progress").And.Contain("7 of 9"));
        });
    }

    /// <summary>A listing whose every read fails transiently is unreadable, not empty, and names the failures.</summary>
    [Test]
    public async Task A_listing_that_only_fails_transiently_is_unreadable()
    {
        var result = await ListingWait.UntilListedAsync<string>(
            "the self-test listing",
            _ => throw new RpcException(new Status(StatusCode.ResourceExhausted, "The backup store is busy.")),
            rows => rows.Count,
            9,
            Stall,
            Ceiling,
            Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Complete, Is.False);
            Assert.That(result.Unreadable, Is.True);
            Assert.That(result.SuccessfulReads, Is.Zero);
            Assert.That(result.FailedReads, Is.GreaterThan(0));
            Assert.That(result.Failure, Does.Contain("no read succeeded").And.Contain("ResourceExhausted"));
        });
    }

    /// <summary>A read that hangs is cut at the stall bound, so the wait never outlasts its own bounds.</summary>
    [Test]
    public async Task A_hanging_read_is_cut_at_the_stall_bound()
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();

        var result = await ListingWait.UntilListedAsync<string>(
            "the self-test listing",
            async cancellationToken =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return [];
            },
            rows => rows.Count,
            9,
            Stall,
            Ceiling,
            Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Unreadable, Is.True);
            Assert.That(result.Failure, Does.Contain("cut at the wait's bound"));
            Assert.That(clock.Elapsed, Is.LessThan(Ceiling));
        });
    }

    /// <summary>A failure that is not transient is a refusal, and propagates.</summary>
    [Test]
    public void A_refusal_propagates()
    {
        var ex = Assert.ThrowsAsync<RpcException>(() => ListingWait.UntilListedAsync<string>(
            "the self-test listing",
            _ => throw new RpcException(new Status(StatusCode.PermissionDenied, "denied")),
            rows => rows.Count,
            9,
            Stall,
            Ceiling,
            Describe));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    /// <summary>A count that keeps rising still stops at the ceiling.</summary>
    [Test]
    public async Task A_rising_count_stops_at_the_ceiling()
    {
        var listed = 0;

        var result = await ListingWait.UntilListedAsync(
            "the self-test listing", _ => Rows(++listed), rows => rows.Count, int.MaxValue, Stall, Stall, Describe);

        Assert.Multiple(() =>
        {
            Assert.That(result.Complete, Is.False);
            Assert.That(result.Failure, Does.Contain("ceiling"));
        });
    }

    private static Task<IReadOnlyList<string>> Rows(int count) =>
        Task.FromResult<IReadOnlyList<string>>(Enumerable.Range(0, count).Select(i => $"row-{i}").ToList());

    private static string Describe(IReadOnlyList<string> rows) => $"{rows.Count} listed";
}
