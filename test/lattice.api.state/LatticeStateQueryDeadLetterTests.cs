using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit tests for the read-only dead-letter-queue surface on
/// <see cref="LatticeStateQuery"/>: <c>GetDeadLetterCountAsync</c> and
/// <c>ListDeadLettersAsync</c>. The query is exercised directly with a
/// substituted dead-letter store, so no cluster is stood up and no timing is
/// involved. Auth-backed visibility is off (no access gate is registered on the
/// substitute service provider), so these assert the store-projection and paging
/// logic in isolation.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryDeadLetterTests
{
    private static LatticeStateQuery CreateQuery(ILatticeSchemaDeadLetterStore? store)
    {
        var services = Substitute.For<IServiceProvider>();
        if (store is not null)
        {
            services.GetService(typeof(ILatticeSchemaDeadLetterStore)).Returns(store);
        }

        var grainFactory = Substitute.For<IGrainFactory>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var apiOptions = Options.Create(new LatticeApiStateOptions());

        return new LatticeStateQuery(grainFactory, options, apiOptions, services, new NullTenantContextResolver());
    }

    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> Entries(
        IEnumerable<LatticeSchemaDeadLetterEntry> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var entry in source)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static LatticeSchemaDeadLetterEntry Entry(
        string key,
        int previewLength = 2,
        int valueByteLength = 2,
        string reason = "reason",
        LatticeSchemaDeadLetterSource source = LatticeSchemaDeadLetterSource.Replication) =>
        new(
            key,
            new byte[previewLength],
            valueByteLength,
            reason,
            source,
            DateTimeOffset.UnixEpoch);

    [Test]
    public async Task GetDeadLetterCountAsync_no_store_registered_returns_zero()
    {
        var query = CreateQuery(store: null);

        var count = await query.GetDeadLetterCountAsync("tree-a");

        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task GetDeadLetterCountAsync_delegates_to_store()
    {
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.CountAsync("tree-a", Arg.Any<CancellationToken>()).Returns(7);
        var query = CreateQuery(store);

        var count = await query.GetDeadLetterCountAsync("tree-a");

        Assert.That(count, Is.EqualTo(7));
    }

    [Test]
    public void GetDeadLetterCountAsync_null_or_empty_tree_throws()
    {
        var query = CreateQuery(store: null);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await query.GetDeadLetterCountAsync(null!),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () => await query.GetDeadLetterCountAsync(string.Empty),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_no_store_registered_returns_empty_page()
    {
        var query = CreateQuery(store: null);

        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void ListDeadLettersAsync_null_request_throws()
    {
        var query = CreateQuery(store: null);

        Assert.That(
            async () => await query.ListDeadLettersAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ListDeadLettersAsync_empty_tree_throws()
    {
        var query = CreateQuery(store: null);

        Assert.That(
            async () => await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = string.Empty }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListDeadLettersAsync_projects_entry_fields()
    {
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries([Entry("k1", previewLength: 2, valueByteLength: 100, reason: "too big", source: LatticeSchemaDeadLetterSource.Restore)]));
        var query = CreateQuery(store);

        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a" });

        Assert.That(page.Entries, Has.Count.EqualTo(1));
        var entry = page.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.EqualTo("k1"));
            Assert.That(entry.ValueByteLength, Is.EqualTo(100));
            Assert.That(entry.ValuePreview, Has.Length.EqualTo(2));
            Assert.That(entry.Reason, Is.EqualTo("too big"));
            Assert.That(entry.Source, Is.EqualTo(DeadLetterSourceKind.Restore));
            Assert.That(entry.PreviewTruncated, Is.True);
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_untruncated_preview_flag_is_false()
    {
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries([Entry("k1", previewLength: 8, valueByteLength: 8)]));
        var query = CreateQuery(store);

        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a" });

        Assert.That(page.Entries[0].PreviewTruncated, Is.False);
    }

    [Test]
    public async Task ListDeadLettersAsync_maps_every_source_kind()
    {
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries(
            [
                Entry("k0", source: LatticeSchemaDeadLetterSource.Replication),
                Entry("k1", source: LatticeSchemaDeadLetterSource.Restore),
                Entry("k2", source: LatticeSchemaDeadLetterSource.LocalRejected),
            ]));
        var query = CreateQuery(store);

        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a" });

        Assert.That(
            page.Entries.Select(e => e.Source),
            Is.EqualTo(new[]
            {
                DeadLetterSourceKind.Replication,
                DeadLetterSourceKind.Restore,
                DeadLetterSourceKind.LocalRejected,
            }));
    }

    [Test]
    public async Task ListDeadLettersAsync_pages_with_offset_cursor()
    {
        var all = Enumerable.Range(0, 5).Select(i => Entry($"k{i}")).ToArray();
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries(all));
        var query = CreateQuery(store);

        var first = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a", PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k0", "k1" }));
            Assert.That(first.NextPageToken, Is.EqualTo("2"));
        });

        var second = await query.ListDeadLettersAsync(
            new DeadLetterQueueRequest { TreeId = "tree-a", PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k2", "k3" }));
            Assert.That(second.NextPageToken, Is.EqualTo("4"));
        });

        var third = await query.ListDeadLettersAsync(
            new DeadLetterQueueRequest { TreeId = "tree-a", PageSize = 2, PageToken = second.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(third.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k4" }));
            Assert.That(third.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_exact_page_boundary_has_no_next_token()
    {
        var all = Enumerable.Range(0, 4).Select(i => Entry($"k{i}")).ToArray();
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries(all));
        var query = CreateQuery(store);

        var page = await query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree-a", PageSize = 4 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(4));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListDeadLettersAsync_malformed_token_reads_from_start()
    {
        var all = Enumerable.Range(0, 3).Select(i => Entry($"k{i}")).ToArray();
        var store = Substitute.For<ILatticeSchemaDeadLetterStore>();
        store.ListAsync("tree-a", Arg.Any<CancellationToken>())
            .Returns(_ => Entries(all));
        var query = CreateQuery(store);

        var page = await query.ListDeadLettersAsync(
            new DeadLetterQueueRequest { TreeId = "tree-a", PageSize = 10, PageToken = "not-a-number" });

        Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "k0", "k1", "k2" }));
    }
}
