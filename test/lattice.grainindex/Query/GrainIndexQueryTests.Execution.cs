namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Execution behaviour: how a planned query walks the tree, pages, cancels, and
/// cleans up the server-side cursor state it opened.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public void Query_defaults_to_a_durable_cursor_and_the_default_page_size()
    {
        var index = Populated();

        var query = index.Index.Where(s => s.Age >= 18);

        Assert.Multiple(() =>
        {
            Assert.That(query.PageSize, Is.EqualTo(GrainIndexQueryDefaults.PageSize));
            Assert.That(query.Execution, Is.EqualTo(GrainIndexQueryDefaults.Execution));
            Assert.That(GrainIndexQueryDefaults.Execution, Is.EqualTo(GrainIndexQueryExecution.DurableCursor));
        });
    }

    [Test]
    public async Task Durable_cursor_execution_opens_and_closes_a_cursor()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
            Assert.That(index.Tree.CursorsOpened, Is.EqualTo(1));
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task Snapshot_cursor_execution_opens_and_closes_a_cursor()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
            Assert.That(index.Tree.CursorsOpened, Is.EqualTo(1));
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task Stream_execution_opens_no_cursor()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.Stream));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
            Assert.That(index.Tree.CursorsOpened, Is.Zero);
        });
    }

    [Test]
    public async Task Stream_execution_matches_the_cursor_result_for_a_payload_scan()
    {
        var index = Populated();

        var streamed = new List<string>();
        await foreach (var match in index.Index
            .Where(s => s.Status == TestStatus.Active)
            .WithExecution(GrainIndexQueryExecution.Stream)
            .ToMatchesAsync())
        {
            streamed.Add(match.GrainKey);
        }

        Assert.That(streamed, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Paging_a_page_at_a_time_returns_the_same_rows()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 0).WithPageSize(1));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task A_multi_range_clause_scans_each_range_and_yields_each_grain_once()
    {
        var index = Populated();

        // '!=' resolves to the two ranges either side of the excluded slot.
        var keys = await KeysAsync(index.Index.Where(s => s.Country != "GB").WithPageSize(1));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "bob", "dave" }));
            Assert.That(index.Tree.CursorsOpened, Is.EqualTo(2));
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task A_query_can_be_enumerated_more_than_once()
    {
        var index = Populated();
        var query = index.Index.Where(s => s.Age >= 18);

        var first = await KeysAsync(query);
        var second = await KeysAsync(query);

        Assert.That(second, Is.EquivalentTo(first));
    }

    [Test]
    public void With_page_size_returns_a_new_query_and_leaves_the_original_alone()
    {
        var index = Populated();
        var query = index.Index.Where(s => s.Age >= 18);

        var paged = query.WithPageSize(7);

        Assert.Multiple(() =>
        {
            Assert.That(paged, Is.Not.SameAs(query));
            Assert.That(paged.PageSize, Is.EqualTo(7));
            Assert.That(query.PageSize, Is.EqualTo(GrainIndexQueryDefaults.PageSize));
        });
    }

    [Test]
    public void With_execution_returns_a_new_query_and_leaves_the_original_alone()
    {
        var index = Populated();
        var query = index.Index.Where(s => s.Age >= 18);

        var streamed = query.WithExecution(GrainIndexQueryExecution.Stream);

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Is.Not.SameAs(query));
            Assert.That(streamed.Execution, Is.EqualTo(GrainIndexQueryExecution.Stream));
            Assert.That(query.Execution, Is.EqualTo(GrainIndexQueryExecution.DurableCursor));
        });
    }

    [Test]
    public void With_execution_carries_the_page_size_through()
    {
        var index = Populated();

        var query = index.Index.Where(s => s.Age >= 18)
            .WithPageSize(3)
            .WithExecution(GrainIndexQueryExecution.Stream);

        Assert.That(query.PageSize, Is.EqualTo(3));
    }

    [Test]
    public void With_page_size_rejects_a_non_positive_page()
    {
        var index = Populated();
        var query = index.Index.Where(s => s.Age >= 18);

        Assert.Throws<ArgumentOutOfRangeException>(() => query.WithPageSize(0));
    }

    [Test]
    public void With_execution_rejects_an_undeclared_mode()
    {
        var index = Populated();
        var query = index.Index.Where(s => s.Age >= 18);

        Assert.Throws<ArgumentOutOfRangeException>(() => query.WithExecution((GrainIndexQueryExecution)99));
    }

    [Test]
    public void Enumeration_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (string _ in index.Index.Where(s => s.Age >= 0).ToKeysAsync(cancellation.Token))
            {
                // The token is already cancelled, so no row is expected.
            }
        });
    }

    [Test]
    public void Draining_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await index.Index.Where(s => s.Age >= 0).ToKeyListAsync(cancellation.Token));
    }

    [Test]
    public void Any_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await index.Index.Where(s => s.Age >= 0).AnyAsync(cancellation.Token));
    }

    [Test]
    public void Grain_enumeration_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in index.Index.Where(s => s.Age >= 0).ToGrainsAsync(cancellation.Token))
            {
                // The token is already cancelled, so no row is expected.
            }
        });
    }

    [Test]
    public void Match_enumeration_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in index.Index.Where(s => s.Age >= 0).ToMatchesAsync(cancellation.Token))
            {
                // The token is already cancelled, so no row is expected.
            }
        });
    }

    [Test]
    public void Grain_list_observes_a_cancelled_token()
    {
        var index = Populated();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await index.Index.Where(s => s.Age >= 0).ToGrainListAsync(cancellation.Token));
    }
}
