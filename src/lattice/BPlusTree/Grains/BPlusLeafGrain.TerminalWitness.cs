using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The durable applied-terminal witness (issue #4545): which keys each saga's
/// terminal settled on this leaf without a marked prepare stamp.
/// <para>
/// A destination-side shadow marker gates a migrated row while its saga is
/// committed (or its decision is masked) and the saga's terminal has not
/// settled the key here. A marker that arrives after the terminal - a delayed
/// shadow forward or sweep replay, after a reactivation, or routed by key to a
/// split sibling that never saw the terminal - would then gate the key until
/// the registry forgets the saga. A marker carrying the saga's marked prepare
/// stamp is released by the read gate's self-check, because a key the terminal
/// settled at that stamp holds a row at or above it. The witness covers the
/// rest: keys settled without such a stamp (a CRDT fold, an unmarked drain, a
/// backstop that carried no stamp) and keys an abort discarded. A key it
/// records is not marked, not carried across a split, and not gated.
/// </para>
/// <para>
/// The witness is kept in memory, per (saga, key), and made durable in this
/// leaf's own sidecar row (<see cref="ILeafTerminalWitnessGrain"/>), never in the
/// leaf state: it is written only when it has changed, and always before a
/// state write, so no persisted projection checkpoint can pass a terminal whose
/// witness is not durable - the write-ahead log, which replay reads past that
/// checkpoint, holds it until then. A failed sidecar write fails the state
/// write with it. Entries are pruned once the registry reads their saga as
/// absent, when every marker for it passes through anyway; nothing else ever
/// removes one, so the witness has no cap and no eviction.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>Minimum interval between two prune passes of the witness on one activation.</summary>
    internal static readonly TimeSpan TerminalWitnessPruneInterval = TimeSpan.FromMinutes(1);

    private Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? _terminalWitness;
    private Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? _pendingWitnessAdds;
    private HashSet<Guid>? _pendingWitnessRemoves;
    private bool _terminalWitnessHydrated;
    private long _lastTerminalWitnessPruneTicks;

    /// <summary>Test seam: replaces <see cref="TerminalWitnessPruneInterval"/> on this activation.</summary>
    internal TimeSpan? TerminalWitnessPruneIntervalOverride { get; set; }

    /// <summary>The sidecar grain, or <see langword="null"/> for a leaf without a Guid key.</summary>
    private ILeafTerminalWitnessGrain? TerminalWitnessSidecar =>
        context.GrainId.TryGetGuidKey(out var leafKey, out _)
            ? grainFactory.GetGrain<ILeafTerminalWitnessGrain>(leafKey)
            : null;

    /// <summary>Whether a change to the witness still has to reach the sidecar.</summary>
    internal bool HasPendingTerminalWitness =>
        _pendingWitnessAdds is { Count: > 0 } || _pendingWitnessRemoves is { Count: > 0 };

    /// <summary>
    /// Loads the sidecar into the index once per activation, before the first
    /// lookup. Every caller is a rare path - a marker install, a shadowed read,
    /// a split - so this costs one call per activation that ever needs it.
    /// </summary>
    internal async ValueTask EnsureTerminalWitnessHydratedAsync()
    {
        if (_terminalWitnessHydrated)
            return;

        var persisted = TerminalWitnessSidecar is { } sidecar ? await sidecar.LoadAsync() : null;
        if (_terminalWitnessHydrated)
            return;
        _terminalWitnessHydrated = true;
        if (persisted is not { Length: > 0 })
            return;

        foreach (var witness in persisted)
        {
            if (witness.Keys is null || (_pendingWitnessRemoves?.Contains(witness.TransactionId) ?? false))
                continue;
            var set = GetOrAddWitness(ref _terminalWitness, witness.TransactionId, witness.RecordedAtTicks);
            foreach (var key in witness.Keys)
            {
                if (!string.IsNullOrEmpty(key))
                    set.Add(key);
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="transactionId"/>'s terminal settled
    /// <paramref name="key"/> on this leaf without a marked prepare stamp - now,
    /// on an earlier activation, or on the donor this leaf was split from. Reads
    /// the index only: callers hydrate it first.
    /// </summary>
    internal bool IsTerminalWitnessed(Guid transactionId, string key) =>
        _terminalWitness is { } index
        && index.TryGetValue(transactionId, out var witness)
        && witness.Keys.Contains(key);

    /// <summary>Records that <paramref name="transactionId"/>'s terminal settled <paramref name="key"/> here.</summary>
    private void RecordTerminalWitness(Guid transactionId, string key)
    {
        if (transactionId == Guid.Empty || string.IsNullOrEmpty(key))
            return;
        AddWitnessKey(transactionId, key, DateTime.UtcNow.Ticks);
    }

    /// <summary>
    /// Records that <paramref name="transactionId"/>'s terminal settled every key
    /// in <paramref name="keys"/> here that <paramref name="include"/> admits.
    /// </summary>
    private void RecordTerminalWitness(Guid transactionId, IEnumerable<string> keys, Func<string, bool>? include = null)
    {
        if (transactionId == Guid.Empty)
            return;
        var now = DateTime.UtcNow.Ticks;
        foreach (var key in keys)
        {
            if (!string.IsNullOrEmpty(key) && (include is null || include(key)))
                AddWitnessKey(transactionId, key, now);
        }
    }

    private void AddWitnessKey(Guid transactionId, string key, long recordedAtTicks)
    {
        if (!GetOrAddWitness(ref _terminalWitness, transactionId, recordedAtTicks).Add(key))
            return;
        GetOrAddWitness(ref _pendingWitnessAdds, transactionId, recordedAtTicks).Add(key);
        _pendingWitnessRemoves?.Remove(transactionId);
    }

    private static HashSet<string> GetOrAddWitness(
        ref Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? map,
        Guid transactionId,
        long recordedAtTicks)
    {
        map ??= new Dictionary<Guid, (HashSet<string>, long)>();
        if (map.TryGetValue(transactionId, out var existing))
            return existing.Keys;

        var keys = new HashSet<string>(StringComparer.Ordinal);
        map[transactionId] = (keys, recordedAtTicks);
        return keys;
    }

    /// <summary>
    /// Makes every change to the witness durable in the sidecar. Called before
    /// every state write: a throw fails that write, so a projection checkpoint
    /// never becomes durable past a terminal whose witness is not.
    /// </summary>
    internal async Task FlushTerminalWitnessAsync()
    {
        if (!HasPendingTerminalWitness)
            return;

        var sidecar = TerminalWitnessSidecar;
        if (sidecar is null)
        {
            _pendingWitnessAdds = null;
            _pendingWitnessRemoves = null;
            return;
        }

        var adds = _pendingWitnessAdds;
        var removes = _pendingWitnessRemoves;
        _pendingWitnessAdds = null;
        _pendingWitnessRemoves = null;
        try
        {
            await sidecar.ApplyAsync(ToWitnesses(adds), removes is { Count: > 0 } ? removes.ToArray() : null);
        }
        catch
        {
            RestorePendingWitness(adds, removes);
            throw;
        }
    }

    private void RestorePendingWitness(
        Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? adds,
        HashSet<Guid>? removes)
    {
        if (adds is not null)
        {
            foreach (var (txid, witness) in adds)
            {
                if (_pendingWitnessRemoves?.Contains(txid) ?? false)
                    continue;
                GetOrAddWitness(ref _pendingWitnessAdds, txid, witness.RecordedAtTicks).UnionWith(witness.Keys);
            }
        }

        if (removes is not null)
        {
            foreach (var txid in removes)
            {
                if (_pendingWitnessAdds is null || !_pendingWitnessAdds.ContainsKey(txid))
                    (_pendingWitnessRemoves ??= []).Add(txid);
            }
        }
    }

    private static AppliedTerminalWitness[]? ToWitnesses(Dictionary<Guid, (HashSet<string> Keys, long RecordedAtTicks)>? map)
    {
        if (map is not { Count: > 0 })
            return null;

        var list = new AppliedTerminalWitness[map.Count];
        var i = 0;
        foreach (var (txid, witness) in map)
        {
            var keys = new string[witness.Keys.Count];
            witness.Keys.CopyTo(keys);
            list[i++] = new AppliedTerminalWitness(txid, keys, witness.RecordedAtTicks);
        }

        return list;
    }

    /// <summary>
    /// The witnesses for the keys at or above <paramref name="splitKey"/>, which a
    /// split moves to the new sibling, or <see langword="null"/> when there are
    /// none. The caller hydrates the index first.
    /// </summary>
    private AppliedTerminalWitness[]? CollectTerminalWitnessesForSibling(string splitKey)
    {
        if (_terminalWitness is not { Count: > 0 } index)
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
    /// Whether <paramref name="current"/> names a (saga, key) pair that
    /// <paramref name="sent"/> does not.
    /// </summary>
    private static bool HasWitnessNotIn(AppliedTerminalWitness[]? current, AppliedTerminalWitness[]? sent)
    {
        if (current is not { Length: > 0 })
            return false;
        if (sent is not { Length: > 0 })
            return true;

        var known = new Dictionary<Guid, HashSet<string>>();
        foreach (var witness in sent)
        {
            if (!known.TryGetValue(witness.TransactionId, out var keys))
            {
                keys = new HashSet<string>(StringComparer.Ordinal);
                known[witness.TransactionId] = keys;
            }

            keys.UnionWith(witness.Keys);
        }

        foreach (var witness in current)
        {
            if (!known.TryGetValue(witness.TransactionId, out var keys))
                return true;
            foreach (var key in witness.Keys)
            {
                if (!keys.Contains(key))
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Adopts a split donor's witnesses for the keys this sibling receives, as
    /// pending changes that the sibling's birth write makes durable. A union, so
    /// a recovery-path re-call is idempotent; the entries are copied, never
    /// retained, because the payload crosses the grain boundary without a deep
    /// copy.
    /// </summary>
    private bool AdoptTerminalWitnesses(AppliedTerminalWitness[]? witnesses)
    {
        if (witnesses is not { Length: > 0 })
            return false;

        var changed = false;
        foreach (var witness in witnesses)
        {
            if (witness.TransactionId == Guid.Empty || witness.Keys is null)
                continue;
            foreach (var key in witness.Keys)
            {
                if (string.IsNullOrEmpty(key) || IsTerminalWitnessed(witness.TransactionId, key))
                    continue;
                AddWitnessKey(witness.TransactionId, key, witness.RecordedAtTicks);
                changed = true;
            }
        }

        return changed;
    }

    /// <summary>
    /// Best-effort prune: asks the registry about every witness recorded at
    /// least <see cref="TerminalWitnessPruneInterval"/> ago, at most once per
    /// interval, and drops those it reads as absent. An absent saga reads
    /// <see cref="TxStatus.InFlight"/>, and a shadow marker for an in-flight saga
    /// passes through the read gate, so a pruned witness can never be needed
    /// again. Any other answer, or a registry fault, keeps the witness. The drop
    /// reaches the sidecar with the next state write.
    /// </summary>
    internal async Task PruneTerminalWitnessAsync()
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return;

        // A leaf that has recorded nothing and never needed its witness this
        // activation is not made to load it just to prune: nothing it could hold
        // grows while the leaf records nothing.
        if (!_terminalWitnessHydrated && _terminalWitness is not { Count: > 0 })
            return;

        var now = DateTime.UtcNow.Ticks;
        var interval = (TerminalWitnessPruneIntervalOverride ?? TerminalWitnessPruneInterval).Ticks;
        if (now - _lastTerminalWitnessPruneTicks < interval)
            return;
        _lastTerminalWitnessPruneTicks = now;

        // Only a hydrated index knows what the sidecar holds. Hydrating costs one
        // load per activation, and only once something has been recorded or the
        // sidecar may hold entries from an earlier activation.
        try
        {
            await EnsureTerminalWitnessHydratedAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return;
        }

        if (_terminalWitness is not { Count: > 0 } index)
            return;

        List<Guid>? aged = null;
        foreach (var (txid, witness) in index)
        {
            if (now - witness.RecordedAtTicks >= interval)
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
            if (statuses.TryGetValue(txid, out var status) && status == TxStatus.InFlight
                && _terminalWitness!.Remove(txid))
            {
                _pendingWitnessAdds?.Remove(txid);
                (_pendingWitnessRemoves ??= []).Add(txid);
            }
        }
    }

    /// <summary>Deletes the sidecar row, for a leaf being removed.</summary>
    private Task ClearTerminalWitnessSidecarAsync() =>
        TerminalWitnessSidecar is { } sidecar ? sidecar.ClearAsync() : Task.CompletedTask;
}
