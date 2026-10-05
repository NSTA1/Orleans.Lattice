namespace Orleans.Lattice.Replication.Grains;

internal sealed class BootstrapReceiverPreCapture
{
    public Dictionary<string, BootstrapCapturedEntry> SourceEntries { get; } = new(StringComparer.Ordinal);

    /// <summary>Source-origin rows of any kind, tombstones and expiring rows included.</summary>
    public int SourceRowCount { get; set; }

    public bool HeldNoSourceRows => SourceRowCount == 0;
}
