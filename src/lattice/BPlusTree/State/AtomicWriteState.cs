namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A pre-saga snapshot of a single key's value, captured during the
/// <see cref="AtomicWritePhase.Prepare"/> phase so that compensation can
/// restore it via a fresh write (LWW resolves in favor of the newer HLC).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicPreValue)]
internal sealed class AtomicPreValue
{
    /// <summary>The key the snapshot belongs to.</summary>
    [Id(0)] public string Key { get; set; } = string.Empty;

    /// <summary>
    /// The value before the saga started, or <c>null</c> if the key was
    /// absent or tombstoned.
    /// </summary>
    [Id(1)] public byte[]? Value { get; set; }

    /// <summary>
    /// <c>true</c> if the key existed (i.e. had a live value) before the saga;
    /// <c>false</c> if it was absent or tombstoned.
    /// </summary>
    [Id(2)] public bool Existed { get; set; }

    /// <summary>
    /// The absolute UTC <c>DateTimeOffset.UtcTicks</c> at which the pre-saga
    /// entry was set to expire ( TTL), or <c>0</c> if the entry had no
    /// TTL. Defaulting to <c>0</c> keeps persisted pre-saga state from earlier
    /// versions wire-compatible (a missing <see cref="Id"/>-3 field decodes
    /// to <c>0</c>, matching a no-TTL entry).
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; set; }

    /// <summary>
    /// Origin cluster id captured from the pre-saga entry's
    /// <see cref="Primitives.LwwValue{T}.OriginClusterId"/>, or <c>null</c>
    /// when the key was absent or authored locally. Restored through
    /// <see cref="LatticeOriginContext.With"/> during compensation so the
    /// rolled-back value re-lands with its original origin stamp.
    /// Wire-compatible: missing field on legacy persisted state decodes
    /// to <c>null</c>.
    /// </summary>
    [Id(4)] public string? OriginClusterId { get; set; }
}

/// <summary>
/// Persistent state for <see cref="Grains.AtomicWriteGrain"/>.
/// Tracks the progress of an in-flight atomic multi-key write so that it
/// can be resumed (or compensated) after a silo restart.
/// Key format: <c>{treeId}/{operationId}</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicWriteState)]
internal sealed class AtomicWriteState
{
    /// <summary>Current lifecycle phase of the saga.</summary>
    [Id(0)] public AtomicWritePhase Phase { get; set; } = AtomicWritePhase.NotStarted;

    /// <summary>
    /// Tree ID the saga writes into. Captured at <see cref="AtomicWritePhase.Prepare"/>
    /// so the grain remains deterministic after activation.
    /// </summary>
    [Id(1)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The entries to write, in the order they are applied. A duplicate-key
    /// check is performed before the saga starts so this list always has
    /// distinct keys.
    /// </summary>
    [Id(2)] public List<KeyValuePair<string, byte[]>> Entries { get; set; } = [];

    /// <summary>
    /// Pre-saga snapshots keyed by the same order as <see cref="Entries"/>.
    /// Populated during <see cref="AtomicWritePhase.Prepare"/>.
    /// </summary>
    [Id(3)] public List<AtomicPreValue> PreValues { get; set; } = [];

    /// <summary>
    /// Index of the next entry to commit during <see cref="AtomicWritePhase.Execute"/>,
    /// or the next entry to roll back during <see cref="AtomicWritePhase.Compensate"/>.
    /// </summary>
    [Id(4)] public int NextIndex { get; set; }

    /// <summary>
    /// Number of consecutive retries on the current step. Reset to zero on advance.
    /// </summary>
    [Id(5)] public int RetriesOnCurrentStep { get; set; }

    /// <summary>
    /// Message of the exception that forced the saga into
    /// <see cref="AtomicWritePhase.Compensate"/>. Preserved for logging and
    /// for the synchronous client path to re-throw a meaningful error.
    /// </summary>
    [Id(6)] public string? FailureMessage { get; set; }

    /// <summary>
    /// SHA-256 fingerprint of the sorted key set submitted when the saga
    /// was first started (caller-supplied idempotency key scenarios). When
    /// non-null, a re-entry to <see cref="Grains.AtomicWriteGrain.ExecuteAsync"/>
    /// whose entries produce a different fingerprint is rejected with
    /// <see cref="InvalidOperationException"/>. Null for legacy persisted
    /// state written before this field existed; absent fingerprint skips
    /// the check (matches prior behaviour).
    /// </summary>
    [Id(7)] public byte[]? KeyFingerprint { get; set; }
}
