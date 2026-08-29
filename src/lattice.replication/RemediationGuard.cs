using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Enforces the operator-tunable guards in front of the automatic anti-entropy
/// remediation repair stage: a per-<c>(tree, peer)</c> remediation-traffic rate
/// cap measured over a deterministic accounting window, and a
/// per-<c>(tree, peer)</c> circuit breaker that disables remediation after a
/// run of consecutive failures and half-opens after a cooldown. The guard also
/// drives the process-wide
/// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
/// observable gauge, which reports value <c>1</c> for each
/// <c>(tree, peer)</c> whose remediation is currently disabled, tagged by the
/// disabling reason.
/// </summary>
/// <remarks>
/// <para>
/// Rate-cap and circuit-breaker accounting is <em>instance</em> state, scoped to
/// the owning digest-probe grain activation (which is per shard/tree), so it
/// does not contend across trees and is reset by a grain reactivation. The
/// methods take explicit <c>nowTicks</c> arguments so the window and cooldown
/// policy are deterministic and unit-testable.
/// </para>
/// <para>
/// The disabled-state gauge, by contrast, is <em>process-wide</em>: its backing
/// store and its single idempotent registration on
/// <see cref="LatticeReplicationMetrics.Meter"/> are static, mirroring the
/// registration model of <see cref="ReplicationPeerStats"/>. Constructing any
/// instance ensures the gauge is registered exactly once per process.
/// </para>
/// <para>
/// This type is an in-process accounting/telemetry helper. It is never sent
/// over the wire nor persisted, so it carries no Orleans serialization
/// attributes.
/// </para>
/// </remarks>
public sealed class RemediationGuard
{
    private static readonly object RegistrationLock = new();
    private static bool _gaugeRegistered;

    // Process-wide disabled-state backing for the observable gauge. A present
    // entry means remediation for that (tree, peer) is currently disabled for
    // the recorded reason; the gauge yields value 1 for each. ClearDisabled
    // removes the entry so no series is emitted once remediation is permitted.
    private static readonly ConcurrentDictionary<DisabledKey, RemediationDisabledReason> DisabledStates = new();

    // Per-activation rate-cap / circuit-breaker accounting, keyed by peer.
    private readonly ConcurrentDictionary<string, PeerAccount> _accounts = new(StringComparer.Ordinal);

    /// <summary>
    /// Initialises a new instance and ensures the process-wide
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// observable gauge is registered on the shared meter. Registration is
    /// idempotent: only the first instance constructed in the process registers
    /// the gauge.
    /// </summary>
    public RemediationGuard()
    {
        lock (RegistrationLock)
        {
            if (!_gaugeRegistered)
            {
                RegisterGauge();
                _gaugeRegistered = true;
            }
        }
    }

    /// <summary>
    /// Attempts to begin a remediation pass for <paramref name="peer"/> against
    /// the per-window entry <paramref name="windowBudget"/>. Rolls the
    /// accounting window over (resetting the consumed-entry counter) when at
    /// least <paramref name="windowTicks"/> have elapsed since the window
    /// opened. The first pass in a fresh window always passes; subsequent passes
    /// pass only while the consumed-entry count is below the budget.
    /// </summary>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="windowBudget">Per-window entry budget (always at least 1).</param>
    /// <param name="windowTicks">Accounting-window length in ticks.</param>
    /// <param name="nowTicks">The current timestamp in ticks.</param>
    /// <returns><see langword="true"/> when the pass may proceed; otherwise <see langword="false"/>.</returns>
    public bool TryBeginRemediation(string peer, int windowBudget, long windowTicks, long nowTicks)
    {
        ArgumentNullException.ThrowIfNull(peer);

        var account = _accounts.GetOrAdd(peer, static (_, now) => new PeerAccount { WindowStartTicks = now }, nowTicks);
        lock (account)
        {
            if (nowTicks - account.WindowStartTicks >= windowTicks)
            {
                account.WindowStartTicks = nowTicks;
                account.ConsumedEntries = 0;
            }

            return account.ConsumedEntries < windowBudget;
        }
    }

    /// <summary>
    /// Records that a remediation pass re-shipped <paramref name="entries"/>
    /// entries to <paramref name="peer"/>, charging them against the current
    /// window budget.
    /// </summary>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="entries">The number of entries re-shipped (may be zero).</param>
    public void RecordEntriesShipped(string peer, int entries)
    {
        ArgumentNullException.ThrowIfNull(peer);

        if (entries <= 0)
        {
            return;
        }

        var account = _accounts.GetOrAdd(peer, static _ => new PeerAccount());
        lock (account)
        {
            account.ConsumedEntries += entries;
        }
    }

    /// <summary>
    /// Returns whether the circuit breaker for <paramref name="peer"/> is open
    /// and still within its cooldown, meaning remediation must be skipped. Once
    /// <paramref name="cooldownTicks"/> have elapsed since the breaker opened
    /// (or since the most recent failed half-open trial), this returns
    /// <see langword="false"/> so a single half-open trial pass is permitted.
    /// </summary>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="cooldownTicks">Circuit-breaker cooldown in ticks.</param>
    /// <param name="nowTicks">The current timestamp in ticks.</param>
    /// <returns><see langword="true"/> when remediation is blocked by the breaker.</returns>
    public bool IsCircuitBlocking(string peer, long cooldownTicks, long nowTicks)
    {
        ArgumentNullException.ThrowIfNull(peer);

        if (!_accounts.TryGetValue(peer, out var account))
        {
            return false;
        }

        lock (account)
        {
            if (!account.CircuitOpen)
            {
                return false;
            }

            return nowTicks - account.CircuitOpenedTicks < cooldownTicks;
        }
    }

    /// <summary>
    /// Records a successful remediation pass for <paramref name="peer"/>:
    /// resets the consecutive-failure count and closes the circuit breaker.
    /// </summary>
    /// <param name="peer">The diverged peer cluster id.</param>
    public void RecordSuccess(string peer)
    {
        ArgumentNullException.ThrowIfNull(peer);

        var account = _accounts.GetOrAdd(peer, static _ => new PeerAccount());
        lock (account)
        {
            account.ConsecutiveFailures = 0;
            account.CircuitOpen = false;
        }
    }

    /// <summary>
    /// Records a failed remediation pass for <paramref name="peer"/>. Opens the
    /// circuit breaker once the consecutive-failure count reaches
    /// <paramref name="failureThreshold"/>; a failure while the breaker is
    /// already open (a failed half-open trial) refreshes the cooldown.
    /// </summary>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="failureThreshold">Consecutive failures that open the breaker (at least 1).</param>
    /// <param name="nowTicks">The current timestamp in ticks.</param>
    /// <returns><see langword="true"/> when the breaker is open after this failure.</returns>
    public bool RecordFailure(string peer, int failureThreshold, long nowTicks)
    {
        ArgumentNullException.ThrowIfNull(peer);

        var account = _accounts.GetOrAdd(peer, static _ => new PeerAccount());
        lock (account)
        {
            account.ConsecutiveFailures++;

            if (account.CircuitOpen)
            {
                // A failed half-open trial re-opens the breaker for a fresh cooldown.
                account.CircuitOpenedTicks = nowTicks;
                return true;
            }

            if (account.ConsecutiveFailures >= failureThreshold)
            {
                account.CircuitOpen = true;
                account.CircuitOpenedTicks = nowTicks;
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Marks remediation for <paramref name="tree"/>/<paramref name="peer"/> as
    /// currently disabled for the supplied <paramref name="reason"/>, so the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge reports value <c>1</c> for that pair with the matching reason tag.
    /// </summary>
    /// <param name="tree">The logical replicated-tree name.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="reason">Why remediation is disabled.</param>
    public static void PublishDisabled(string tree, string peer, RemediationDisabledReason reason)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        DisabledStates[new DisabledKey(tree, peer)] = reason;
    }

    /// <summary>
    /// Clears any disabled state for <paramref name="tree"/>/<paramref name="peer"/>
    /// so the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge emits no series for that pair (remediation is permitted).
    /// </summary>
    /// <param name="tree">The logical replicated-tree name.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    public static void ClearDisabled(string tree, string peer)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        DisabledStates.TryRemove(new DisabledKey(tree, peer), out _);
    }

    private static void RegisterGauge()
    {
        LatticeReplicationMetrics.Meter.CreateObservableGauge<long>(
            LatticeReplicationMetrics.DigestRemediationDisabledName,
            ObserveDisabled,
            unit: "{state}",
            description: "Each (tree, peer) for which automatic anti-entropy remediation is currently disabled, valued 1 and tagged by reason.");
    }

    private static IEnumerable<Measurement<long>> ObserveDisabled()
    {
        foreach (var kv in DisabledStates)
        {
            yield return new Measurement<long>(
                1,
                new System.Diagnostics.TagList
                {
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, kv.Key.Tree),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, kv.Key.Peer),
                    new KeyValuePair<string, object?>(
                        LatticeReplicationMetrics.TagReason,
                        LatticeReplicationMetrics.DigestRemediationDisabledReasonTag(kv.Value)),
                    LatticeTenantLabel.ForTree(kv.Key.Tree),
                });
        }
    }

    private readonly record struct DisabledKey(string Tree, string Peer);

    private sealed class PeerAccount
    {
        public long WindowStartTicks;
        public long ConsumedEntries;
        public int ConsecutiveFailures;
        public bool CircuitOpen;
        public long CircuitOpenedTicks;
    }
}
