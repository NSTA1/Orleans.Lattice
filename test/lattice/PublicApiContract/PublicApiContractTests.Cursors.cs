namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── Key cursors ─────────────────────────────────────────────────────

    [Test]
    public async Task OpenKeyCursorAsync_returns_a_handle_then_NextKeysAsync_drains_pages()
    {
        var tree = Tree("pac-cursors-keys-pages");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4"), Kvp("e", "5")]);

        var cursor = await tree.OpenKeyCursorAsync();
        try
        {
            var collected = new List<string>();
            while (true)
            {
                var page = await tree.NextKeysAsync(cursor, pageSize: 2);
                collected.AddRange(page.Keys);
                if (!page.HasMore)
                {
                    break;
                }
            }

            Assert.That(collected, Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    [Test]
    public async Task OpenKeyCursorAsync_with_range_filters_to_range()
    {
        var tree = Tree("pac-cursors-keys-range");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var cursor = await tree.OpenKeyCursorAsync(startInclusive: "b", endExclusive: "d");
        try
        {
            var page = await tree.NextKeysAsync(cursor, pageSize: 100);
            Assert.That(page.Keys, Is.EqualTo(new[] { "b", "c" }));
            Assert.That(page.HasMore, Is.False);
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    [Test]
    public async Task OpenKeyCursorAsync_in_reverse_returns_descending_keys()
    {
        var tree = Tree("pac-cursors-keys-reverse");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var cursor = await tree.OpenKeyCursorAsync(reverse: true);
        try
        {
            var page = await tree.NextKeysAsync(cursor, pageSize: 100);
            Assert.That(page.Keys, Is.EqualTo(new[] { "c", "b", "a" }));
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    [Test]
    public async Task NextKeysAsync_returns_empty_page_with_HasMore_false_when_drained()
    {
        var tree = Tree("pac-cursors-keys-drained");
        await tree.SetAsync("k", Bytes("v"));

        var cursor = await tree.OpenKeyCursorAsync();
        try
        {
            var first = await tree.NextKeysAsync(cursor, pageSize: 100);
            Assert.That(first.Keys, Is.EqualTo(new[] { "k" }));
            // Drain: subsequent call returns an empty page.
            var second = await tree.NextKeysAsync(cursor, pageSize: 100);
            Assert.That(second.HasMore, Is.False);
            Assert.That(second.Keys, Is.Empty);
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    // ── Entry cursors ───────────────────────────────────────────────────

    [Test]
    public async Task OpenEntryCursorAsync_returns_a_handle_then_NextEntriesAsync_drains_pages()
    {
        var tree = Tree("pac-cursors-entries-pages");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var cursor = await tree.OpenEntryCursorAsync();
        try
        {
            var page = await tree.NextEntriesAsync(cursor, pageSize: 100);
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(page.Entries.Select(e => Str(e.Value)), Is.EqualTo(new[] { "1", "2", "3" }));
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    // ── Delete-range cursor ─────────────────────────────────────────────

    [Test]
    public async Task OpenDeleteRangeCursorAsync_steps_through_range_and_completes()
    {
        var tree = Tree("pac-cursors-deleterange");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4"), Kvp("e", "5")]);

        var cursor = await tree.OpenDeleteRangeCursorAsync("b", "e");
        try
        {
            var totalDeleted = 0;
            while (true)
            {
                var step = await tree.DeleteRangeStepAsync(cursor, maxToDelete: 2);
                totalDeleted += step.DeletedThisStep;
                if (step.IsComplete)
                {
                    break;
                }
            }

            Assert.That(totalDeleted, Is.EqualTo(3));

            // Confirm only b/c/d are tombstoned.
            Assert.That(await tree.GetAsync("a"), Is.Not.Null);
            Assert.That(await tree.GetAsync("b"), Is.Null);
            Assert.That(await tree.GetAsync("c"), Is.Null);
            Assert.That(await tree.GetAsync("d"), Is.Null);
            Assert.That(await tree.GetAsync("e"), Is.Not.Null);
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    [Test]
    public async Task DeleteRangeStepAsync_after_completion_is_an_idempotent_noop()
    {
        var tree = Tree("pac-cursors-deleterange-idempotent");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2")]);
        var cursor = await tree.OpenDeleteRangeCursorAsync("a", "z");
        try
        {
            // Drain in one step.
            var first = await tree.DeleteRangeStepAsync(cursor, maxToDelete: 100);
            Assert.That(first.IsComplete, Is.True);

            // Second call after completion is a no-op.
            var second = await tree.DeleteRangeStepAsync(cursor, maxToDelete: 100);
            Assert.That(second.IsComplete, Is.True);
            Assert.That(second.DeletedThisStep, Is.EqualTo(0));
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    // ── Wrong-cursor-kind ───────────────────────────────────────────────

    [Test]
    public async Task NextEntriesAsync_with_a_key_cursor_throws()
    {
        var tree = Tree("pac-cursors-wrongkind-key");
        await tree.SetAsync("a", Bytes("1"));

        var cursor = await tree.OpenKeyCursorAsync();
        try
        {
            Assert.That(
                async () => await tree.NextEntriesAsync(cursor, pageSize: 10),
                Throws.InstanceOf<InvalidOperationException>());
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    [Test]
    public async Task NextKeysAsync_with_an_entry_cursor_throws()
    {
        var tree = Tree("pac-cursors-wrongkind-entry");
        await tree.SetAsync("a", Bytes("1"));

        var cursor = await tree.OpenEntryCursorAsync();
        try
        {
            Assert.That(
                async () => await tree.NextKeysAsync(cursor, pageSize: 10),
                Throws.InstanceOf<InvalidOperationException>());
        }
        finally
        {
            await tree.CloseCursorAsync(cursor);
        }
    }

    // ── CloseCursorAsync idempotency ────────────────────────────────────

    [Test]
    public async Task CloseCursorAsync_is_idempotent_for_unknown_cursors()
    {
        var tree = Tree("pac-cursors-close-idempotent");
        // Closing a never-opened cursor id is a no-op.
        await tree.CloseCursorAsync("never-opened-cursor-id");

        // "No-op" is a claim about state, not just about not throwing: the
        // unknown id must not have been registered as a live cursor by the
        // close itself, so paging it still reports it as unknown.
        Assert.That(
            async () => await tree.NextKeysAsync("never-opened-cursor-id", pageSize: 10),
            Throws.InstanceOf<InvalidOperationException>(),
            "closing an unknown cursor id must not create one");
    }

    [Test]
    public async Task CloseCursorAsync_is_idempotent_for_already_closed_cursors()
    {
        var tree = Tree("pac-cursors-close-twice");
        var cursor = await tree.OpenKeyCursorAsync();
        await tree.CloseCursorAsync(cursor);
        await tree.CloseCursorAsync(cursor);

        // The second close must leave the cursor closed rather than resurrect
        // it; without this the test passes even if the repeat close silently
        // re-registered the id.
        Assert.That(
            async () => await tree.NextKeysAsync(cursor, pageSize: 10),
            Throws.InstanceOf<InvalidOperationException>(),
            "a twice-closed cursor must stay closed");
    }
}
