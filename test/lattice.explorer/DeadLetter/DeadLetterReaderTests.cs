using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Tests.DeadLetter;

/// <summary>
/// Unit tests for <see cref="DeadLetterReader"/>: request shaping, projection of
/// the wire <see cref="DeadLetterEntryRecord"/> to the explorer view model, and
/// the argument guards. The state-API client is substituted so no gRPC server is
/// needed.
/// </summary>
[TestFixture]
public class DeadLetterReaderTests
{
    private static ILatticeStateClient ClientReturningCount(int count)
    {
        var client = Substitute.For<ILatticeStateClient>();
        client
            .GetDeadLetterCountAsync(Arg.Any<DeadLetterCountRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new DeadLetterCountResponse
            {
                TreeId = ci.Arg<DeadLetterCountRequest>().TreeId,
                Count = count,
            }));
        return client;
    }

    private static ILatticeStateClient ClientReturningPage(DeadLetterQueuePage page)
    {
        var client = Substitute.For<ILatticeStateClient>();
        client
            .ListDeadLettersAsync(Arg.Any<DeadLetterQueueRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(page));
        return client;
    }

    [Test]
    public async Task CountAsync_returns_the_client_count()
    {
        var reader = new DeadLetterReader(ClientReturningCount(7));

        var count = await reader.CountAsync("tree-1");

        Assert.That(count, Is.EqualTo(7));
    }

    [Test]
    public async Task CountAsync_forwards_the_tree_id()
    {
        var client = ClientReturningCount(0);
        var reader = new DeadLetterReader(client);

        await reader.CountAsync("tree-xyz");

        await client.Received(1).GetDeadLetterCountAsync(
            Arg.Is<DeadLetterCountRequest>(r => r.TreeId == "tree-xyz"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void CountAsync_rejects_null_tree_id()
    {
        var reader = new DeadLetterReader(ClientReturningCount(0));

        Assert.That(
            async () => await reader.CountAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void CountAsync_rejects_empty_tree_id()
    {
        var reader = new DeadLetterReader(ClientReturningCount(0));

        Assert.That(
            async () => await reader.CountAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ListAsync_shapes_the_request_with_tree_id_and_page_size()
    {
        var client = ClientReturningPage(new DeadLetterQueuePage());
        var reader = new DeadLetterReader(client);

        await reader.ListAsync("tree-1", pageSize: 25);

        await client.Received(1).ListDeadLettersAsync(
            Arg.Is<DeadLetterQueueRequest>(r => r.TreeId == "tree-1" && r.PageSize == 25 && r.PageToken == null),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListAsync_passes_a_non_empty_continuation_token()
    {
        var client = ClientReturningPage(new DeadLetterQueuePage());
        var reader = new DeadLetterReader(client);

        await reader.ListAsync("tree-1", pageSize: 25, continuationToken: "cursor-2");

        await client.Received(1).ListDeadLettersAsync(
            Arg.Is<DeadLetterQueueRequest>(r => r.PageToken == "cursor-2"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListAsync_normalises_an_empty_continuation_token_to_null()
    {
        var client = ClientReturningPage(new DeadLetterQueuePage());
        var reader = new DeadLetterReader(client);

        await reader.ListAsync("tree-1", pageSize: 25, continuationToken: string.Empty);

        await client.Received(1).ListDeadLettersAsync(
            Arg.Is<DeadLetterQueueRequest>(r => r.PageToken == null),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListAsync_projects_entries_and_preserves_the_preview_bytes()
    {
        var page = new DeadLetterQueuePage
        {
            Entries = new[]
            {
                new DeadLetterEntryRecord
                {
                    Key = "k1",
                    ValuePreview = new byte[] { 1, 2, 3 },
                    ValueByteLength = 9,
                    PreviewTruncated = true,
                    Reason = "schema mismatch",
                    Source = DeadLetterSourceKind.Replication,
                    TimestampUtc = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
                },
            },
            NextPageToken = "next",
        };
        var reader = new DeadLetterReader(ClientReturningPage(page));

        var result = await reader.ListAsync("tree-1", pageSize: 25);

        Assert.That(result.Entries, Has.Count.EqualTo(1));
        var entry = result.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.EqualTo("k1"));
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(entry.ValueByteLength, Is.EqualTo(9));
            Assert.That(entry.Truncated, Is.True);
            Assert.That(entry.Reason, Is.EqualTo("schema mismatch"));
            Assert.That(entry.Source, Is.EqualTo(DeadLetterSource.Replication));
            Assert.That(entry.TimestampUtc, Is.EqualTo(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)));
            Assert.That(result.ContinuationToken, Is.EqualTo("next"));
            Assert.That(result.HasMore, Is.True);
        });
    }

    [Test]
    public async Task ListAsync_maps_every_source_kind()
    {
        var page = new DeadLetterQueuePage
        {
            Entries = new[]
            {
                Record("k0", DeadLetterSourceKind.Replication),
                Record("k1", DeadLetterSourceKind.Restore),
                Record("k2", DeadLetterSourceKind.LocalRejected),
                Record("k3", DeadLetterSourceKind.Unknown),
            },
        };
        var reader = new DeadLetterReader(ClientReturningPage(page));

        var result = await reader.ListAsync("tree-1", pageSize: 25);

        Assert.That(
            result.Entries.Select(e => e.Source),
            Is.EqualTo(new[]
            {
                DeadLetterSource.Replication,
                DeadLetterSource.Restore,
                DeadLetterSource.LocalRejected,
                DeadLetterSource.Unknown,
            }));
    }

    [Test]
    public async Task ListAsync_returns_an_empty_page_when_the_queue_is_empty()
    {
        var reader = new DeadLetterReader(ClientReturningPage(new DeadLetterQueuePage()));

        var result = await reader.ListAsync("tree-1", pageSize: 25);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Is.Empty);
            Assert.That(result.ContinuationToken, Is.Null);
            Assert.That(result.HasMore, Is.False);
        });
    }

    [Test]
    public void ListAsync_rejects_null_tree_id()
    {
        var reader = new DeadLetterReader(ClientReturningPage(new DeadLetterQueuePage()));

        Assert.That(
            async () => await reader.ListAsync(null!, pageSize: 25),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ListAsync_rejects_empty_tree_id()
    {
        var reader = new DeadLetterReader(ClientReturningPage(new DeadLetterQueuePage()));

        Assert.That(
            async () => await reader.ListAsync(string.Empty, pageSize: 25),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_rejects_a_null_client()
    {
        Assert.That(() => new DeadLetterReader(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    private static DeadLetterEntryRecord Record(string key, DeadLetterSourceKind source) => new()
    {
        Key = key,
        ValuePreview = Array.Empty<byte>(),
        ValueByteLength = 0,
        Reason = "r",
        Source = source,
        TimestampUtc = DateTimeOffset.UnixEpoch,
    };
}
