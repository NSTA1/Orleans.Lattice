namespace Orleans.Lattice.Replication;

/// <summary>
/// The decision a receiver makes about one causal dependency <c>(o, t)</c> -
/// origin <c>o</c>'s write at HLC <c>t</c> - once its exact identity was not
/// found among the applied writes it remembers (issue #4586).
/// <para>
/// <c>S</c> is the low watermark origin <c>o</c> ships: every write of
/// <c>o</c> stamped strictly below <c>S</c> has been acknowledged by this
/// receiver. Acknowledged means applied, or held: parked in a causal-apply
/// buffer, parked in a dead-letter queue, or marked lost. So the write is
/// applied exactly when <c>t &lt; S</c> and <c>(o, t)</c> is not held. A lost
/// write is never applied, so its dependents are dead-lettered. This is the
/// per-identity form of the check: folding the held identities into a minimum
/// would let one permanent lost mark pin the low watermark forever.
/// </para>
/// </summary>
internal static class CausalFrontierCore
{
    /// <summary>The verdict for a dependency at <paramref name="required"/>.</summary>
    /// <param name="required">The HLC of the named write.</param>
    /// <param name="lowWatermark">The origin's recorded low watermark, or zero when none was received.</param>
    /// <param name="held">Whether this receiver holds the named write without having applied it.</param>
    /// <param name="lost">Whether this receiver marked the named write lost.</param>
    public static CausalDependencyVerdict Decide(HybridLogicalClock required, HybridLogicalClock lowWatermark, bool held, bool lost)
    {
        if (lost)
        {
            return CausalDependencyVerdict.Lost;
        }

        return required < lowWatermark && !held ? CausalDependencyVerdict.Met : CausalDependencyVerdict.Unmet;
    }
}
