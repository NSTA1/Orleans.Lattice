namespace Orleans.Lattice.Replication.Grains;

internal sealed class BootstrapReceiverPreCapture
{
    public Dictionary<string, BootstrapCapturedEntry> SourceEntries { get; } = new(StringComparer.Ordinal);

    public int RawEntryCount { get; set; }

    public bool IsEmpty => RawEntryCount == 0;
}
