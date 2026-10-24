using NSubstitute;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

[TestFixture]
public sealed class BackupArtifactStreamTests
{
    [Test]
    public async Task ReadAsync_array_overload_respects_offset_count_and_partial_chunks()
    {
        await using var stream = new BackupArtifactStream(Chunks());
        byte[] buffer = [99, 99, 99, 99, 99];

        var first = await stream.ReadAsync(buffer, 1, 2, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(2));
            Assert.That(buffer, Is.EqualTo(new byte[] { 99, 1, 2, 99, 99 }));
            Assert.That(stream.Position, Is.EqualTo(2));
        });

        var second = await stream.ReadAsync(buffer, 3, 2, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(second, Is.EqualTo(1), "the remaining byte of this chunk is returned before advancing");
            Assert.That(buffer, Is.EqualTo(new byte[] { 99, 1, 2, 3, 99 }));
            Assert.That(stream.Position, Is.EqualTo(3));
        });

        Assert.That(await stream.ReadAsync(buffer, 0, 5, CancellationToken.None), Is.EqualTo(2));
        Assert.That(buffer[..2], Is.EqualTo(new byte[] { 4, 5 }));
        Assert.That(await stream.ReadAsync(buffer, 0, 5, CancellationToken.None), Is.Zero);
        Assert.That(await stream.ReadAsync(buffer, 0, 5, CancellationToken.None), Is.Zero);
        Assert.That(stream.Position, Is.EqualTo(5));
    }

    [Test]
    public void Read_synchronous_overload_reads_chunks_and_disposes_the_enumerator_once()
    {
        var chunks = Substitute.For<IAsyncEnumerable<ReadOnlyMemory<byte>>>();
        var enumerator = Substitute.For<IAsyncEnumerator<ReadOnlyMemory<byte>>>();
        chunks.GetAsyncEnumerator(Arg.Any<CancellationToken>()).Returns(enumerator);
        enumerator.MoveNextAsync().Returns(new ValueTask<bool>(true), new ValueTask<bool>(false));
        enumerator.Current.Returns(new ReadOnlyMemory<byte>(new byte[] { 7, 8 }));
        var stream = new BackupArtifactStream(chunks);
        var buffer = new byte[4];

        Assert.Multiple(() =>
        {
            Assert.That(stream.Read(buffer, 1, 2), Is.EqualTo(2));
            Assert.That(buffer, Is.EqualTo(new byte[] { 0, 7, 8, 0 }));
            Assert.That(stream.Read(buffer, 0, 4), Is.Zero);
        });
        stream.Dispose();
        stream.Dispose();

        enumerator.Received(1).DisposeAsync();
    }

    [Test]
    public async Task DisposeAsync_followed_by_synchronous_dispose_releases_the_enumerator_once()
    {
        var chunks = Substitute.For<IAsyncEnumerable<ReadOnlyMemory<byte>>>();
        var enumerator = Substitute.For<IAsyncEnumerator<ReadOnlyMemory<byte>>>();
        chunks.GetAsyncEnumerator(Arg.Any<CancellationToken>()).Returns(enumerator);
        using var cancellation = new CancellationTokenSource();
        var stream = new BackupArtifactStream(chunks, cancellation.Token);

        await stream.DisposeAsync();
        await stream.DisposeAsync();
        stream.Dispose();

        chunks.Received(1).GetAsyncEnumerator(cancellation.Token);
        await enumerator.Received(1).DisposeAsync();
    }

    [Test]
    public async Task ReadAsync_empty_buffer_does_not_advance_and_cancelled_read_does_not_consume_a_chunk()
    {
        var chunks = Substitute.For<IAsyncEnumerable<ReadOnlyMemory<byte>>>();
        var enumerator = Substitute.For<IAsyncEnumerator<ReadOnlyMemory<byte>>>();
        chunks.GetAsyncEnumerator(Arg.Any<CancellationToken>()).Returns(enumerator);
        await using var stream = new BackupArtifactStream(chunks);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(await stream.ReadAsync(Memory<byte>.Empty, cancelled.Token), Is.Zero);
        Assert.That(async () => await stream.ReadAsync(new byte[1].AsMemory(), cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(stream.Position, Is.Zero);
        await enumerator.DidNotReceive().MoveNextAsync();
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks()
    {
        await Task.CompletedTask;
        yield return ReadOnlyMemory<byte>.Empty;
        yield return new byte[] { 1, 2, 3 };
        yield return ReadOnlyMemory<byte>.Empty;
        yield return new byte[] { 4, 5 };
    }
}
