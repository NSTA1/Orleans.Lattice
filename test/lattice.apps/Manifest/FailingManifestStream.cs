namespace Orleans.Lattice.Apps.Tests;

internal sealed class FailingManifestStream : MemoryStream
{
    public override int Read(Span<byte> buffer) => throw new IOException("Injected resource read failure.");
    public override int Read(byte[] buffer, int offset, int count) => throw new IOException("Injected resource read failure.");
}
