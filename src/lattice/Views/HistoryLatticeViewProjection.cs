using System.IO.Hashing;
using Orleans.Lattice.Views;

namespace Orleans.Lattice;

/// <summary>
/// Built-in <see cref="ILatticeViewProjection"/> that turns each source mutation
/// into an append-only durable revision row, the substrate for per-key history.
/// It re-keys every mutation to <c>{sourceKey}/{encodedHlc}</c> (a fixed-width,
/// chronologically-sortable suffix) and emits a single <see cref="ViewWriteKind.Upsert"/>
/// carrying a serialized <see cref="HistoryRow"/>. Distinct source HLCs map to
/// distinct view keys, so nothing folds and the full timeline is retained
/// durably - independently of source write-ahead-log garbage collection.
/// <para>
/// <b>Append-only.</b> A delete is itself a revision, so it is recorded as a
/// <see cref="HistoryRowKind.Delete"/> row rather than removing prior revisions.
/// <see cref="ViewWrite.SourceKey"/> is left unset on every emit so the
/// maintainer's re-key collision detector is skipped (history keys, carrying the
/// HLC, never collide).
/// </para>
/// <para>
/// <b>Compact by construction.</b> A CRDT mutation is stored as its author delta
/// (<see cref="LatticeMutation.Delta"/>) - the compact, doubling-free history the
/// element-level provenance decoder reads. An LWW <see cref="MutationKind.Set"/>
/// row carries the full value <em>at projection time</em>; the maintainer then
/// shapes it per the source tree's live <see cref="HistoryRetentionMode"/> (the
/// projection stays a pure function of the single mutation and never reads the
/// runtime-tunable policy).
/// </para>
/// <para>
/// <b>Range deletes.</b> A predicate-filtered range delete carries
/// <see cref="LatticeMutation.MatchedKeys"/> and lowers to one exact
/// <see cref="HistoryRowKind.Delete"/> revision per matched key. An unconstrained
/// range delete emits a <see cref="ViewWriteKind.RangeReconcile"/>; on an
/// accumulative history view the maintainer records it as a
/// <see cref="HistoryRowKind.RangeTombstone"/> marker rather than rebuilding,
/// because in an append-only log a range delete does not erase the fact that
/// prior values existed.
/// </para>
/// </summary>
public sealed class HistoryLatticeViewProjection : ILatticeViewProjection
{
    /// <summary>The stable code-identity version of the history projection logic.</summary>
    public const string Version = "history-v1";

    private readonly HistoryRowCodec _codec;

    /// <summary>
    /// Creates the history projection. The <paramref name="codec"/> is resolved
    /// from the silo service provider (registered by <c>AddLatticeViews</c>), so a
    /// runtime-created history view re-hydrates after a restart without
    /// configuration.
    /// </summary>
    /// <param name="codec">Serialises the emitted <see cref="HistoryRow"/> into the view entry value.</param>
    internal HistoryLatticeViewProjection(HistoryRowCodec codec)
    {
        ArgumentNullException.ThrowIfNull(codec);
        _codec = codec;
    }

    /// <inheritdoc />
    public string ProjectionVersion => Version;

    /// <inheritdoc />
    public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                yield return EmitRow(mutation.Key, BuildPointRow(mutation), mutation.Timestamp);
                break;

            case MutationKind.Delete:
            case MutationKind.Tombstone:
                yield return EmitRow(mutation.Key, BuildDeleteRow(mutation.Key, mutation), mutation.Timestamp);
                break;

            case MutationKind.DeleteRange:
                if (mutation.MatchedKeys is { Count: > 0 } matched)
                {
                    // Predicate-filtered: each matched key gets an exact per-key
                    // delete revision, re-keyed by its own HLC suffix.
                    foreach (var key in matched)
                    {
                        yield return EmitRow(key, BuildDeleteRow(key, mutation), mutation.Timestamp);
                    }

                    yield break;
                }

                if (string.IsNullOrEmpty(mutation.EndExclusiveKey))
                {
                    // No bound to record; nothing actionable.
                    yield break;
                }

                // Unconstrained: the maintainer records a range-tombstone marker
                // for an accumulative view (it does not rebuild). RangeReconcile is
                // the signal; the EndKey bounds the swept range.
                yield return ViewWrite.RangeReconcile(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                break;

            default:
                yield break;
        }
    }

    private ViewWrite EmitRow(string sourceKey, in HistoryRow row, HybridLogicalClock timestamp)
    {
        // SourceKey is deliberately unset: history keys carry the HLC and never
        // collide, so the re-key collision detector must skip them. The maintainer
        // stamps ExpiresAtTicks from the retention window at drain time.
        return ViewWrite.Upsert(HistoryKey.Encode(sourceKey, timestamp), _codec.Encode(row), timestamp);
    }

    private static HistoryRow BuildPointRow(in LatticeMutation mutation)
    {
        if (mutation.Delta is { } delta)
        {
            // CRDT: store the author delta only (the compact, doubling-free
            // history), carrying the convergence mode for the provenance decoder.
            return new HistoryRow
            {
                Timestamp = mutation.Timestamp,
                Kind = HistoryRowKind.CrdtDelta,
                SourceKey = mutation.Key,
                OriginClusterId = mutation.OriginClusterId,
                Delta = delta,
                Mode = mutation.Mode,
            };
        }

        // LWW: carry the full value plus its fingerprint. The maintainer decides
        // whether the bytes survive (full-value / recent hybrid) or are stripped
        // to the fingerprint (metadata-only) per the live retention mode.
        var value = mutation.Value;
        return new HistoryRow
        {
            Timestamp = mutation.Timestamp,
            Kind = HistoryRowKind.Set,
            SourceKey = mutation.Key,
            OriginClusterId = mutation.OriginClusterId,
            Value = value,
            ValueHash = value is null ? 0 : unchecked((long)XxHash64.HashToUInt64(value)),
            ValueLength = value?.Length ?? 0,
            Mode = mutation.Mode,
        };
    }

    private static HistoryRow BuildDeleteRow(string sourceKey, in LatticeMutation mutation) => new()
    {
        Timestamp = mutation.Timestamp,
        Kind = HistoryRowKind.Delete,
        SourceKey = sourceKey,
        OriginClusterId = mutation.OriginClusterId,
        Mode = mutation.Mode,
    };
}
