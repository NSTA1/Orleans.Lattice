using System.Buffers.Binary;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Fast unit tests for the provider-agnostic snapshot framing: the versioned
/// header, the length-prefixed record frames, and the writer / reader round trip.
/// These need no cluster - they exercise the serialization and format-header
/// logic directly over an in-memory stream and a minimal Orleans serializer.
/// </summary>
[TestFixture]
public sealed class RepoContextSnapshotFormatTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public async Task WriteHeaderAsync_then_ReadHeaderAsync_returns_current_version()
    {
        using var stream = new MemoryStream();
        await RepoContextSnapshotFormat.WriteHeaderAsync(
            stream, RepoContextSnapshotFormat.CurrentVersion, TestContext.CurrentContext.CancellationToken);
        stream.Position = 0;

        var version = await RepoContextSnapshotFormat.ReadHeaderAsync(
            stream, TestContext.CurrentContext.CancellationToken);

        Assert.That(version, Is.EqualTo(RepoContextSnapshotFormat.CurrentVersion));
    }

    [Test]
    public void ReadHeaderAsync_bad_magic_throws_invalid_data()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 1, 0, 0, 0 });

        Assert.That(
            () => RepoContextSnapshotFormat.ReadHeaderAsync(
                stream, TestContext.CurrentContext.CancellationToken).AsTask(),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public async Task ReadHeaderAsync_unsupported_version_throws_invalid_data()
    {
        using var stream = new MemoryStream();
        // Valid magic but a version far beyond what this build can read.
        await RepoContextSnapshotFormat.WriteHeaderAsync(
            stream, RepoContextSnapshotFormat.CurrentVersion + 99, TestContext.CurrentContext.CancellationToken);
        stream.Position = 0;

        Assert.That(
            () => RepoContextSnapshotFormat.ReadHeaderAsync(
                stream, TestContext.CurrentContext.CancellationToken).AsTask(),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public async Task ReadFrameAsync_returns_null_at_clean_end_of_stream()
    {
        using var stream = new MemoryStream();
        await RepoContextSnapshotFormat.WriteHeaderAsync(
            stream, RepoContextSnapshotFormat.CurrentVersion, TestContext.CurrentContext.CancellationToken);
        stream.Position = 0;
        await RepoContextSnapshotFormat.ReadHeaderAsync(stream, TestContext.CurrentContext.CancellationToken);

        var frame = await RepoContextSnapshotFormat.ReadFrameAsync(
            stream, TestContext.CurrentContext.CancellationToken);

        Assert.That(frame, Is.Null);
    }

    [Test]
    public void ReadFrameAsync_truncated_length_prefix_throws_invalid_data()
    {
        // Two bytes where a four-byte length prefix is required.
        using var stream = new MemoryStream(new byte[] { 7, 0 });

        Assert.That(
            () => RepoContextSnapshotFormat.ReadFrameAsync(
                stream, TestContext.CurrentContext.CancellationToken).AsTask(),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public void ReadFrameAsync_truncated_payload_throws_invalid_data()
    {
        // A length prefix that claims 16 bytes, but only 4 follow.
        var buffer = new byte[sizeof(int) + 4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, 16);
        using var stream = new MemoryStream(buffer);

        Assert.That(
            () => RepoContextSnapshotFormat.ReadFrameAsync(
                stream, TestContext.CurrentContext.CancellationToken).AsTask(),
            Throws.InstanceOf<InvalidDataException>());
    }

    [Test]
    public async Task Writer_then_reader_round_trips_records_in_order()
    {
        var records = new[]
        {
            new RepoContextSnapshotRecord { Key = "repo/acme/file/a.cs", Value = [1, 2, 3] },
            new RepoContextSnapshotRecord
            {
                Key = "repo/acme/file/b.cs",
                Value = [4, 5],
                Vector = [9, 8, 7],
                EmbeddingSpace = "onyx-v1",
            },
        };

        using var stream = new MemoryStream();
        var writer = new RepoContextSnapshotWriter(stream, _serializer);
        foreach (var record in records)
        {
            await writer.WriteRecordAsync(record, TestContext.CurrentContext.CancellationToken);
        }

        stream.Position = 0;
        var reader = new RepoContextSnapshotReader(stream, _serializer);
        var readBack = new List<RepoContextSnapshotRecord>();
        await foreach (var record in reader.ReadAsync(TestContext.CurrentContext.CancellationToken))
        {
            readBack.Add(record);
        }

        Assert.Multiple(() =>
        {
            Assert.That(reader.FormatVersion, Is.EqualTo(RepoContextSnapshotFormat.CurrentVersion));
            Assert.That(readBack, Has.Count.EqualTo(2));
            Assert.That(readBack[0].Key, Is.EqualTo("repo/acme/file/a.cs"));
            Assert.That(readBack[0].Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(readBack[0].Vector, Is.Null);
            Assert.That(readBack[1].Key, Is.EqualTo("repo/acme/file/b.cs"));
            Assert.That(readBack[1].Vector, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(readBack[1].EmbeddingSpace, Is.EqualTo("onyx-v1"));
        });
    }

    [Test]
    public async Task Writer_emits_a_valid_header_for_an_empty_snapshot()
    {
        using var stream = new MemoryStream();
        var writer = new RepoContextSnapshotWriter(stream, _serializer);
        await writer.WriteHeaderAsync(TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var reader = new RepoContextSnapshotReader(stream, _serializer);
        var count = 0;
        await foreach (var _ in reader.ReadAsync(TestContext.CurrentContext.CancellationToken))
        {
            count++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.Zero);
            Assert.That(reader.FormatVersion, Is.EqualTo(RepoContextSnapshotFormat.CurrentVersion));
        });
    }
}
