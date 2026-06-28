namespace Orleans.Lattice.Views;

/// <summary>
/// Accumulative (durable-history) view maintenance: the drain-time reshaping that
/// turns the projection's maximal revision rows into the rows actually stored,
/// and the conversion of an unconstrained range reconcile into an append-only
/// range-tombstone marker. Both run only for a view whose registration is
/// <see cref="ViewRegistration.Accumulative"/>.
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <summary>
    /// Rewrites each <see cref="ViewWriteKind.RangeReconcile"/> in
    /// <paramref name="writes"/> into an <see cref="ViewWriteKind.Upsert"/> of a
    /// <see cref="HistoryRowKind.RangeTombstone"/> marker row, so an accumulative
    /// view records the swept range as a revision instead of rebuilding. Other
    /// writes pass through untouched.
    /// </summary>
    private void ConvertRangeReconcilesToMarkers(List<ViewWrite> writes)
    {
        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            if (write.Kind != ViewWriteKind.RangeReconcile)
            {
                continue;
            }

            var marker = new HistoryRow
            {
                Timestamp = write.Timestamp,
                Kind = HistoryRowKind.RangeTombstone,
                SourceKey = write.Key,
                EndKey = write.EndKey,
            };

            var key = HistoryKey.Encode(write.Key, write.Timestamp);
            writes[i] = ViewWrite.Upsert(key, historyRowCodec.Encode(marker), write.Timestamp);
        }
    }

    /// <summary>
    /// Shapes every revision row in <paramref name="writes"/> for storage under the
    /// source tree's live retention policy: stamps the age-bound expiry and strips
    /// LWW value bytes to metadata per the active <see cref="HistoryRetentionMode"/>.
    /// The policy and the drain clock are read once for the whole pass. The
    /// projection emits the maximal row (so it stays a pure function of one
    /// mutation); the reshaping is the seam where the runtime-tunable policy is
    /// applied.
    /// </summary>
    private async Task ShapeHistoryWritesAsync(List<ViewWrite> writes, string sourceTreeId)
    {
        var policy = await optionsResolver
            .GetHistoryRetentionAsync(sourceTreeId, Options.HistoryHybridFullValueWindow);
        var nowTicks = DateTime.UtcNow.Ticks;

        for (var i = 0; i < writes.Count; i++)
        {
            writes[i] = ShapeHistoryWrite(writes[i], policy, nowTicks);
        }
    }

    /// <summary>
    /// Shapes a single revision <paramref name="write"/> under <paramref name="policy"/>:
    /// decodes the carried <see cref="HistoryRow"/>, strips its LWW value bytes to
    /// metadata per the active mode and stamps the age-bound expiry, then re-encodes.
    /// A non-Upsert write or one carrying no value (a delete or range-tombstone
    /// marker) passes through untouched.
    /// </summary>
    private ViewWrite ShapeHistoryWrite(ViewWrite write, HistoryRetentionPolicy policy, long nowTicks)
    {
        if (write.Kind != ViewWriteKind.Upsert || write.Value is null)
        {
            return write;
        }

        var row = historyRowCodec.Decode(write.Value);
        var (shaped, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, nowTicks);
        return ViewWrite.Upsert(write.Key, historyRowCodec.Encode(shaped), write.Timestamp, expiresAtTicks);
    }
}
