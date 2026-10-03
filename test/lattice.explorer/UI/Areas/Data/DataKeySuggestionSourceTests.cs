using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The key picker's bounded prefix scan, against the data reader alone: the
/// cursor it opens is released, that release is best effort, the caller's own
/// cancellation is let out, and every other fault degrades the field to free text
/// rather than failing the render.
/// </summary>
/// <remarks>
/// Most arms below are fail-closed refusals that answer exactly what a healthy
/// empty tree answers, so a page cannot tell them apart: an unreadable tree and an
/// empty one both draw no suggestions, and a cursor whose release faulted looks
/// identical to one released cleanly. They are reachable only from a fixture
/// against the unit, with a reader that can be made to fault.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataKeySuggestionSourceTests
{
    private const string Tree = "orders";

    private readonly IDataReader _reader = Substitute.For<IDataReader>();

    private DataKeySuggestionSource Source => new(_reader, Tree);

    [Test]
    public async Task The_scan_cursor_the_query_opened_is_released()
    {
        Scan("cursor-1", "order/1", "order/2");
        var released = new TaskCompletionSource();
        _reader.CancelScanAsync(Tree, "cursor-1", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                released.TrySetResult();
                return Task.CompletedTask;
            });

        await Source.SuggestAsync("order/", 5, CancellationToken.None);

        await TestPoll.UntilAsync(() => released.Task.IsCompleted, "the scan cursor is released");
    }

    [Test]
    public async Task A_drained_scan_leaves_no_cursor_to_release()
    {
        Scan(continuation: null, "order/1");
        var released = false;
        _reader.CancelScanAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                released = true;
                return Task.CompletedTask;
            });

        var answer = await Source.SuggestAsync("order/", 5, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(answer.Truncated, Is.False);
            Assert.That(
                await TestPoll.TryUntilAsync(() => released, TimeSpan.FromMilliseconds(250)),
                Is.False,
                "nothing is cancelled when the scan drained itself");
        });
    }

    [Test]
    public async Task A_cursor_release_that_faults_never_reaches_the_caller()
    {
        Scan("cursor-1", "order/1");
        var attempted = new TaskCompletionSource();
        _reader.CancelScanAsync(Tree, "cursor-1", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                attempted.TrySetResult();
                return Task.FromException(new InvalidOperationException("the cursor was already reaped"));
            });

        var answer = await Source.SuggestAsync("order/", 5, CancellationToken.None);

        await TestPoll.UntilAsync(() => attempted.Task.IsCompleted, "the release was attempted");
        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.True, "a best-effort release never fails the query");
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "order/1" }));
        });
    }

    [Test]
    public void The_callers_own_cancellation_is_let_out_rather_than_read_as_unavailable()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        ScanThrows(call => new OperationCanceledException(call.Arg<CancellationToken>()));

        Assert.That(
            async () => await Source.SuggestAsync("order/", 5, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "an abandoned keystroke is not a tree the caller cannot read");
    }

    [Test]
    public async Task A_cancellation_the_caller_did_not_ask_for_still_degrades_the_field_to_free_text()
    {
        // The server gave up on its own scan; the caller's token is untouched, so the
        // filtered rethrow does not apply and the field must stay usable.
        ScanThrows(_ => new OperationCanceledException("the server gave up"));

        var answer = await Source.SuggestAsync("order/", 5, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(DataKeySuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task A_tree_the_caller_cannot_scan_answers_unavailable()
    {
        ScanThrows(_ => new InvalidOperationException("refused"));

        var answer = await Source.SuggestAsync("order/", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.IsAvailable, Is.False);
            Assert.That(answer.Items, Is.Empty);
            Assert.That(answer.UnavailableReason, Is.EqualTo(DataKeySuggestionSource.UnavailableReason));
        });
    }

    [Test]
    public async Task No_more_than_the_limit_is_offered_and_the_overflow_marks_the_answer_truncated()
    {
        Scan(continuation: null, "order/1", "order/2", "order/3", "order/4");

        var answer = await Source.SuggestAsync("order/", 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "order/1", "order/2", "order/3" }));
            Assert.That(answer.Truncated, Is.True, "the limit+1 probe row says more keys match");
        });
    }

    [Test]
    public async Task A_page_that_fills_the_limit_exactly_is_not_truncated()
    {
        Scan(continuation: null, "order/1", "order/2", "order/3");

        var answer = await Source.SuggestAsync("order/", 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items, Has.Count.EqualTo(3));
            Assert.That(answer.Truncated, Is.False);
        });
    }

    [Test]
    public async Task An_undrained_cursor_marks_the_answer_truncated_even_when_the_page_was_short()
    {
        Scan("cursor-1", "order/1");

        var answer = await Source.SuggestAsync("order/", 5, CancellationToken.None);

        Assert.That(answer.Truncated, Is.True, "a cursor that is still open means more keys match");
    }

    [TestCase(3, 4)]
    [TestCase(1, 2)]
    [TestCase(0, 1)]
    [TestCase(400, DataPaging.MaxPageSize)]
    public async Task One_more_row_than_the_limit_is_asked_for_and_clamped_to_the_page_ceiling(int limit, int expected)
    {
        Scan(continuation: null);

        await Source.SuggestAsync("order/", limit, CancellationToken.None);

        await _reader.Received(1).ScanAsync(
            Tree,
            expected,
            Arg.Any<string?>(),
            Arg.Any<TagFilter?>(),
            "order/",
            Arg.Any<EntryScanMode>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_query_with_no_text_at_all_is_refused()
    {
        Assert.That(
            async () => await Source.SuggestAsync(null!, 5, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void The_source_names_the_tree_the_state_api_reads_it_under()
    {
        Assert.That(Source.StateId, Is.EqualTo(Tree));
    }

    private void Scan(string? continuation, params string[] keys) =>
        _reader.ScanAsync(Tree, Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<TagFilter?>(), Arg.Any<string?>(), Arg.Any<EntryScanMode>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DataPage
            {
                Entries = [.. keys.Select(key => new DataEntry { Key = key })],
                ContinuationToken = continuation,
            }));

    private void ScanThrows(Func<NSubstitute.Core.CallInfo, Exception> fault) =>
        _reader.ScanAsync(Tree, Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<TagFilter?>(), Arg.Any<string?>(), Arg.Any<EntryScanMode>(), Arg.Any<CancellationToken>())
            .Throws(fault);
}
