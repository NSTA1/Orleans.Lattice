using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The durable applied-terminal witness (issue #4545): which keys each saga's
/// terminal settled on this leaf.
/// <para>
/// A destination-side shadow marker gates a migrated row while its saga is
/// committed (or its decision is masked) and the saga's terminal has not reached
/// this leaf. The activation-scoped terminal memory cannot answer that last part
/// across a reactivation or on a split sibling, and a marker that arrives after
/// the terminal - a delayed shadow forward or sweep replay - then gates the key
/// until the registry forgets the saga. The witness answers it durably and per
/// key: a key it records already incorporates the saga, so a marker for it is
/// not installed, not carried across a split, and does not gate a read.
/// </para>
/// <para>
/// The index is loaded lazily from <see cref="LeafNodeState.AppliedTerminalWitnesses"/>
/// and written back to it before every state write, so the persisted copy is
/// atomic with the projection checkpoint; replay of the WAL after that
/// checkpoint rebuilds the rest through the same recording calls the foreground
/// path makes. Entries are pruned once the registry reads the saga as absent,
/// at which point every marker for it passes through anyway.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>Minimum interval between two prune passes of the witness on one activation.</summary>
    internal static readonly TimeSpan TerminalWitnessPruneInterval = TimeSpan.FromMinutes(1);

    private Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? _terminalWitness;
    private bool _terminalWitnessHydrated;
    private bool _terminalWitnessDirty;
    private List<AppliedTerminalWitness>? _materialisedTerminalWitnesses;
    private long _lastTerminalWitnessPruneTicks;
    private long _terminalWitnessBytes;
    private bool _terminalWitnessPruneDue;

    /// <summary>
    /// Estimated byte budget of one leaf's witness (issue #4545). A witness lives
    /// until the registry prunes its saga's row - on a busy tree about the
    /// decision retention plus one prune interval - so its size is the rate of
    /// sagas settling keys on the leaf times that lifetime, which is unbounded by
    /// construction; this caps what every state write carries.
    /// </summary>
    internal const long DefaultTerminalWitnessBudgetBytes = 64 * 1024;

    private const long WitnessEntryOverheadBytes = 32;
    private const long WitnessKeyOverheadBytes = 8;

    /// <summary>Test seam: replaces <see cref="DefaultTerminalWitnessBudgetBytes"/> on this activation.</summary>
    internal long? TerminalWitnessBudgetOverride { get; set; }

    private long TerminalWitnessBudget => TerminalWitnessBudgetOverride ?? DefaultTerminalWitnessBudgetBytes;

    /// <summary>Sagas whose witness this activation evicted to hold the budget, for tests.</summary>
    internal int TerminalWitnessEvictions { get; private set; }

    /// <summary>Test seam: replaces <see cref="TerminalWitnessPruneInterval"/> on this activation.</summary>
    internal TimeSpan? TerminalWitnessPruneIntervalOverride { get; set; }

    /// <summary>The witness index, hydrated from the persisted state on first use.</summary>
    private Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? TerminalWitnessIndex
    {
        get
        {
            if (!_terminalWitnessHydrated)
            {
                _terminalWitnessHydrated = true;
                if (state.State.AppliedTerminalWitnesses is { Count: > 0 } persisted)
                {
                    _terminalWitness = new Dictionary<Guid, (HashSet<string>, long)>(persisted.Count);
                    foreach (var witness in persisted)
                    {
                        if (witness.Keys is not null)
                            MergeWitness(witness.TransactionId, witness.Keys, witness.RecordedAtTicks);
                    }
                }

                _materialisedTerminalWitnesses = state.State.AppliedTerminalWitnesses;
                _terminalWitnessDirty = false;
            }

            return _terminalWitness;
        }
    }

    /// <summary>Number of sagas with a recorded witness, for tests.</summary>
    internal int TerminalWitnessCount => TerminalWitnessIndex?.Count ?? 0;

    /// <summary>
    /// Whether <paramref name="transactionId"/>'s terminal settled
    /// <paramref name="key"/> on this leaf, now or on an earlier activation, or
    /// on the donor this leaf was split from.
    /// </summary>
    internal bool IsTerminalWitnessed(Guid transactionId, string key) =>
        TerminalWitnessIndex is { } index
        && index.TryGetValue(transactionId, out var witness)
        && witness.Keys.Contains(key);

    /// <summary>Records that <paramref name="transactionId"/>'s terminal settled <paramref name="key"/> here.</summary>
    private void RecordTerminalWitness(Guid transactionId, string key)
    {
        if (transactionId == Guid.Empty || string.IsNullOrEmpty(key))
            return;
        _ = TerminalWitnessIndex;
        MergeWitness(transactionId, [key], DateTime.UtcNow.Ticks);
    }

    /// <summary>Records that <paramref name="transactionId"/>'s terminal settled every key in <paramref name="keys"/> here.</summary>
    private void RecordTerminalWitness(Guid transactionId, IEnumerable<string> keys, IReadOnlySet<string>? except = null)
    {
        if (transactionId == Guid.Empty)
            return;
        _ = TerminalWitnessIndex;
        HashSet<string>? set = null;
        foreach (var key in keys)
        {
            if (string.IsNullOrEmpty(key) || (except is not null && except.Contains(key)))
                continue;
            set ??= GetOrAddWitness(transactionId, DateTime.UtcNow.Ticks);
            AddWitnessKey(set, key);
        }
    }

    private void MergeWitness(Guid transactionId, IEnumerable<string> keys, long recordedAtTicks)
    {
        var set = GetOrAddWitness(transactionId, recordedAtTicks);
        foreach (var key in keys)
        {
            if (!string.IsNullOrEmpty(key))
                AddWitnessKey(set, key);
        }
    }

    private HashSet<string> GetOrAddWitness(Guid transactionId, long recordedAtTicks)
    {
        _terminalWitness ??= new Dictionary<Guid, (HashSet<string>, long)>();
        if (_terminalWitness.TryGetValue(transactionId, out var existing))
        {
            return existing.Keys;
        }

        var keys = new HashSet<string>(StringComparer.Ordinal);
        _terminalWitness[transactionId] = (keys, recordedAtTicks);
        _terminalWitnessBytes += WitnessEntryOverheadBytes;
        _terminalWitnessDirty = true;
        return keys;
    }

    private void AddWitnessKey(HashSet<string> set, string key)
    {
        if (!set.Add(key))
            return;
        _terminalWitnessDirty = true;
        _terminalWitnessBytes += WitnessKeyBytes(key);
        if (_terminalWitnessBytes > TerminalWitnessBudget)
            _terminalWitnessPruneDue = true;
    }

    private static long WitnessKeyBytes(string key) => WitnessKeyOverheadBytes + 2L * key.Length;

    private static long WitnessBytes(HashSet<string> keys)
    {
        var bytes = WitnessEntryOverheadBytes;
        foreach (var key in keys)
            bytes += WitnessKeyBytes(key);
        return bytes;
    }

    private void RemoveWitness(Guid transactionId)
    {
        if (_terminalWitness is not null && _terminalWitness.Remove(transactionId, out var removed))
        {
            _terminalWitnessBytes -= WitnessBytes(removed.Keys);
            _terminalWitnessDirty = true;
        }
    }

    /// <summary>
    /// Holds the witness to <see cref="TerminalWitnessBudget"/> by evicting the
    /// oldest sagas' entries, and says so (issue #4545). Eviction fails closed:
    /// a key with no witness keeps the read gate, so a delayed marker for it is
    /// declined - the pre-#4545 behaviour, bounded by the registry retiring the
    /// saga - and never served torn. Reached only after a prune that ignored its
    /// throttle could not bring the witness under budget.
    /// </summary>
    private void EnforceTerminalWitnessBudget()
    {
        if (_terminalWitness is not { Count: > 0 } index || _terminalWitnessBytes <= TerminalWitnessBudget)
            return;

        var oldestFirst = new List<(Guid Txid, long RecordedAtTicks)>(index.Count);
        foreach (var (txid, witness) in index)
            oldestFirst.Add((txid, witness.RecordedAtTicks));
        oldestFirst.Sort(static (a, b) => a.RecordedAtTicks.CompareTo(b.RecordedAtTicks));

        var evicted = 0;
        foreach (var (txid, _) in oldestFirst)
        {
            if (_terminalWitnessBytes <= TerminalWitnessBudget)
                break;
            RemoveWitness(txid);
            evicted++;
        }

        TerminalWitnessEvictions += evicted;
        ResolveLogger()?.LogWarning(
            "Leaf {Leaf} on tree {Tree} evicted the applied-terminal witness of {Evicted} saga(s) to hold it to its "
            + "{Budget}-byte budget; the registry still reports them. A delayed shadow marker for one of their keys "
            + "now keeps that key's read gate until the registry retires the saga (reads decline, never tear). "
            + "Sustained eviction means more sagas settle keys on this leaf within the decision retention window "
            + "than the budget holds.",
            context.GrainId,
            state.State.TreeId,
            evicted,
            TerminalWitnessBudget);
    }

    /// <summary>
    /// Writes the index back to <see cref="LeafNodeState.AppliedTerminalWitnesses"/>
    /// when it changed, or when the state object no longer holds the list this
    /// activation last wrote (a re-read replaced it), so the state write about to
    /// happen persists it.
    /// </summary>
    internal void MaterialiseTerminalWitnessForPersist()
    {
        if (!_terminalWitnessHydrated)
            return;
        EnforceTerminalWitnessBudget();
        if (!_terminalWitnessDirty
            && ReferenceEquals(state.State.AppliedTerminalWitnesses, _materialisedTerminalWitnesses))
            return;
        _terminalWitnessDirty = false;

        if (_terminalWitness is not { Count: > 0 } index)
        {
            state.State.AppliedTerminalWitnesses = null;
            _materialisedTerminalWitnesses = null;
            return;
        }

        var list = new List<AppliedTerminalWitness>(index.Count);
        foreach (var (txid, witness) in index)
        {
            if (witness.Keys.Count == 0)
                continue;
            var keys = new string[witness.Keys.Count];
            witness.Keys.CopyTo(keys);
            list.Add(new AppliedTerminalWitness(txid, keys, witness.RecordedAtTicks));
        }

        _materialisedTerminalWitnesses = list.Count > 0 ? list : null;
        state.State.AppliedTerminalWitnesses = _materialisedTerminalWitnesses;
    }

    /// <summary>
    /// The witnesses for the keys at or above <paramref name="splitKey"/>, which a
    /// split moves to the new sibling, or <see langword="null"/> when there are none.
    /// </summary>
    private AppliedTerminalWitness[]? CollectTerminalWitnessesForSibling(string splitKey)
    {
        if (TerminalWitnessIndex is not { Count: > 0 } index)
            return null;

        List<AppliedTerminalWitness>? moved = null;
        foreach (var (txid, witness) in index)
        {
            List<string>? keys = null;
            foreach (var key in witness.Keys)
            {
                if (string.CompareOrdinal(key, splitKey) >= 0)
                    (keys ??= []).Add(key);
            }

            if (keys is not null)
                (moved ??= []).Add(new AppliedTerminalWitness(txid, keys.ToArray(), witness.RecordedAtTicks));
        }

        return moved?.ToArray();
    }

    /// <summary>
    /// Adopts a split donor's witnesses for the keys this sibling receives. A
    /// union, so a recovery-path re-call is idempotent; the entries are copied,
    /// never retained, because the payload crosses the grain boundary without a
    /// deep copy.
    /// </summary>
    private bool AdoptTerminalWitnesses(AppliedTerminalWitness[]? witnesses)
    {
        if (witnesses is not { Length: > 0 })
            return false;

        _ = TerminalWitnessIndex;
        var before = _terminalWitnessDirty;
        _terminalWitnessDirty = false;
        foreach (var witness in witnesses)
        {
            if (witness.TransactionId == Guid.Empty || witness.Keys is null)
                continue;
            MergeWitness(witness.TransactionId, witness.Keys, witness.RecordedAtTicks);
        }

        var changed = _terminalWitnessDirty;
        _terminalWitnessDirty |= before;
        return changed;
    }

    /// <summary>
    /// Best-effort prune: asks the registry about every witness recorded at
    /// least <see cref="TerminalWitnessPruneInterval"/> ago, at most once per
    /// interval - or, once the witness is over its budget, about every witness at
    /// once - and drops those it reads as absent. An absent saga reads
    /// <see cref="TxStatus.InFlight"/>, and a shadow marker for an in-flight saga
    /// passes through the read gate, so a pruned witness can never be needed
    /// again. Any other answer, or a registry fault, keeps the witness.
    /// </summary>
    internal async Task PruneTerminalWitnessAsync()
    {
        if (TerminalWitnessIndex is not { Count: > 0 } index)
            return;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return;

        // Over budget, the throttle and the age filter are skipped: every
        // witness is asked about before the budget evicts anything.
        var now = DateTime.UtcNow.Ticks;
        var interval = (TerminalWitnessPruneIntervalOverride ?? TerminalWitnessPruneInterval).Ticks;
        var overBudget = _terminalWitnessPruneDue;
        if (!overBudget && now - _lastTerminalWitnessPruneTicks < interval)
            return;
        _lastTerminalWitnessPruneTicks = now;
        _terminalWitnessPruneDue = false;

        List<Guid>? aged = null;
        foreach (var (txid, witness) in index)
        {
            if (overBudget || now - witness.RecordedAtTicks >= interval)
                (aged ??= []).Add(txid);
        }

        if (aged is null)
            return;

        Dictionary<Guid, TxStatus> statuses;
        try
        {
            statuses = await TxRegistryFanOut.GetStatusManyAsync(grainFactory, treeId, aged);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var logger = ResolveLogger();
            if (logger is not null && logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(ex, "Could not read saga decisions to prune the applied-terminal witness of tree '{TreeId}'; keeping it.", treeId);
            }

            return;
        }

        foreach (var txid in aged)
        {
            if (statuses.TryGetValue(txid, out var status) && status == TxStatus.InFlight)
                RemoveWitness(txid);
        }
    }
}
