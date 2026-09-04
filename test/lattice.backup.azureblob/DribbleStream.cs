namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// A read-only stream that hands back at most <see cref="MaxBytesPerRead"/> bytes per
/// read, however large the caller's buffer is.
/// <para>
/// A short read is legal on any network stream and the artifact reader is written to
/// tolerate one: a chunk's 4-byte length prefix may arrive split across two reads, and
/// the reader completes the prefix before trusting it. Against a local emulator the
/// whole prefix always arrives at once, so that completion path never runs and a
/// regression in it - trusting a half-read prefix, or reading the remainder into the
/// wrong offset - would decode a garbage frame length and silently corrupt a restore
/// only under real network conditions. Forcing the split makes the tolerated case the
/// tested case.
/// </para>
/// </summary>
internal sealed class DribbleStream(Stream inner, int maxBytesPerRead = 1) : Stream
{
    /// <summary>The ceiling applied to every read length.</summary>
    public int MaxBytesPerRead { get; } = maxBytesPerRead > 0
        ? maxBytesPerRead
        : throw new ArgumentOutOfRangeException(nameof(maxBytesPerRead));

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        inner.Read(buffer, offset, Math.Min(count, MaxBytesPerRead));

    public override int Read(Span<byte> buffer) =>
        inner.Read(buffer[..Math.Min(buffer.Length, MaxBytesPerRead)]);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.ReadAsync(buffer[..Math.Min(buffer.Length, MaxBytesPerRead)], cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        inner.ReadAsync(buffer, offset, Math.Min(count, MaxBytesPerRead), cancellationToken);

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }
}
