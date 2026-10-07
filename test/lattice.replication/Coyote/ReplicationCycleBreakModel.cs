using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// Which cycle-break a <see cref="ReplicationCycleBreakModel"/> run removes.
/// </summary>
public enum ReplicationCycleBreakMode
{
    /// <summary>
    /// The fix: shippers send only what <see cref="ReplicationShipEligibility.IsShipEligible"/>
    /// admits, and receivers drop what <see cref="ReplicationReceiveDedup.IsOwnOrigin"/> flags.
    /// </summary>
    BothGuards,

    /// <summary>The shipper's local-origin filter removed: every WAL entry ships.</summary>
    NoShipFilter,

    /// <summary>The receiver-side cycle-break removed: an own-origin entry is applied.</summary>
    NoReceiverGuard,

    /// <summary>
    /// The anti-vacuity witness: both guards, plus an assertion that no shipper ever drains a
    /// foreign-origin entry from its WAL. Coyote must refute it, which proves the run reaches the
    /// multi-hop state the shipper's filter exists for.
    /// </summary>
    ForeignEntryProbe,
}

/// <summary>
/// A Coyote model of the cycle-break over a multi-hop topology, driving the real
/// <see cref="ReplicationShipEligibility.IsShipEligible"/> (the shipper's filter,
/// <c>ReplicationShipperGrain.ShouldShip</c>) and
/// <see cref="ReplicationReceiveDedup.IsOwnOrigin"/> (the receiver's,
/// <c>ReplicationApplier.ApplyAsync</c>). It is the executable counterpart of the TLA+
/// properties <c>NoRelay</c> and <c>NoReflection</c> in <c>spec/replication/Replication.tla</c>.
/// <para>
/// Clusters a and b author one write each and ship to each other (a two-cluster cycle) and to
/// c; a -&gt; b -&gt; c is a multi-hop path production does not use. A receiver appends what it
/// applies to its own WAL with the authoring origin preserved, exactly as the WAL captures an
/// apply, so every shipper's WAL holds foreign entries its filter must refuse. Besides the
/// honest shippers, an echo peer - a peer on another build, or a hand-built apply pipeline -
/// may hand a cluster its own write back, which is what makes the receiver guard load-bearing.
/// The scheduler interleaves authoring, shipping, echoes and applies.
/// </para>
/// </summary>
public sealed class ReplicationCycleBreakModel : ICoyoteModel
{
    private static readonly string[] Clusters = ["a", "b", "c"];
    private static readonly (string From, string To)[] Peers = [("a", "b"), ("b", "a"), ("a", "c"), ("b", "c")];

    private readonly ReplicationCycleBreakMode _mode;

    /// <summary>Creates the model with one guard configuration.</summary>
    public ReplicationCycleBreakModel(ReplicationCycleBreakMode mode) => _mode = mode;

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var wal = Clusters.ToDictionary(c => c, _ => new List<string>(), StringComparer.Ordinal);
        var cursor = Peers.ToDictionary(p => p, _ => 0);
        var inbox = new List<(string To, string From, string Origin)>();
        var authors = new List<string> { "a", "b" };
        var echoes = 1;

        for (var step = 0; step < 16; step++)
        {
            var choice = Choose(runtime, 4);
            if (choice == 0 && authors.Count > 0)
            {
                var author = authors[0];
                authors.RemoveAt(0);
                wal[author].Add(author);
            }
            else if (choice == 1)
            {
                var (from, to) = Peers[Choose(runtime, Peers.Length)];
                var log = wal[from];
                var at = cursor[(from, to)];
                if (at < log.Count)
                {
                    var origin = log[at];
                    cursor[(from, to)] = at + 1;
                    if (_mode == ReplicationCycleBreakMode.ForeignEntryProbe)
                    {
                        Specification.Assert(
                            string.Equals(origin, from, StringComparison.Ordinal),
                            "PROBE: a shipper drained a foreign-origin entry from its WAL");
                    }

                    // Pinned: every entry is a Set. The core's tombstone-reap clause refuses a
                    // maintenance record whatever its origin, so it can only remove relays and
                    // reflections, never add one; ReplicationShipEligibilityTests covers it.
                    var ships = _mode == ReplicationCycleBreakMode.NoShipFilter
                        || ReplicationShipEligibility.IsShipEligible(origin, MutationKind.Set, from);
                    if (ships)
                    {
                        Specification.Assert(
                            origin == from || origin == to,
                            $"NoRelay: {from} relayed {origin}'s write to {to}");
                        inbox.Add((to, from, origin));
                    }
                }
            }
            else if (choice == 2 && echoes > 0)
            {
                // The echo peer hands a cluster a write of its own back.
                var target = Clusters[Choose(runtime, 2)];
                if (wal[target].Contains(target))
                {
                    echoes--;
                    inbox.Add((target, target == "a" ? "b" : "a", target));
                }
            }
            else if (inbox.Count > 0)
            {
                var index = Choose(runtime, inbox.Count);
                var (to, _, origin) = inbox[index];
                inbox.RemoveAt(index);
                var dropped = _mode != ReplicationCycleBreakMode.NoReceiverGuard
                    && ReplicationReceiveDedup.IsOwnOrigin(origin, to);
                if (!dropped)
                {
                    Specification.Assert(
                        !string.Equals(origin, to, StringComparison.Ordinal),
                        $"NoReflection: {to} applied its own write received from a peer");
                    if (!wal[to].Contains(origin))
                    {
                        wal[to].Add(origin);
                    }
                }
            }
        }
    }

    private static int Choose(ICoyoteRuntime runtime, int count)
    {
        for (var i = 0; i < count - 1; i++)
        {
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return count - 1;
    }
}
