namespace Orleans.Lattice.Schema;

/// <summary>
/// Builds <see cref="LatticeSchemaDeadLetterEntry"/> records for the schema write
/// interceptors. A replicated item that carries its source identity gets a
/// timestamp taken from the source write and is stamped with that identity, so a
/// re-delivery of the same source write files the same record (#5714).
/// </summary>
internal static class SchemaDeadLetterEntries
{
    internal static LatticeSchemaDeadLetterEntry Build(
        string key,
        byte[] value,
        string reason,
        LatticeSchemaDeadLetterSource source,
        int previewMaxBytes,
        TimeProvider timeProvider,
        string? originClusterId,
        HybridLogicalClock? sourceHlc)
    {
        var previewLength = Math.Min(value.Length, previewMaxBytes);
        var preview = previewLength == 0 ? Array.Empty<byte>() : value.AsSpan(0, previewLength).ToArray();
        if (originClusterId is not null && sourceHlc is { } hlc)
        {
            return new LatticeSchemaDeadLetterEntry(
                key, preview, value.Length, reason, source, SchemaDeadLetterKey.IdentityTimestamp(hlc))
            {
                OriginClusterId = originClusterId,
                SourceHlc = hlc,
            };
        }

        return new LatticeSchemaDeadLetterEntry(
            key, preview, value.Length, reason, source, timeProvider.GetUtcNow());
    }
}
