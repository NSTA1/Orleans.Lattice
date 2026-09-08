using System.Reflection;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal resolved view of <see cref="LatticeOptions"/> that re-surfaces the
/// structural sizing fields (<see cref="MaxLeafKeys"/>, <see cref="MaxInternalChildren"/>,
/// <see cref="ShardCount"/>) sourced from the tree registry pin rather than
/// from <c>IOptionsMonitor&lt;LatticeOptions&gt;</c>.
/// <para>
/// Produced exclusively by <see cref="LatticeOptionsResolver.ResolveAsync"/>.
/// Consumers type their local <c>options</c> variable as
/// <see cref="ResolvedLatticeOptions"/> so existing call-site syntax
/// (<c>options.MaxLeafKeys</c>, <c>options.ShardCount</c>) keeps compiling.
/// </para>
/// </summary>
internal sealed class ResolvedLatticeOptions : LatticeOptions
{
    /// <summary>Maximum number of keys per leaf node before a split is triggered. Pinned in the registry.</summary>
    public required int MaxLeafKeys { get; init; }

    /// <summary>Maximum number of children per internal node before a split is triggered. Pinned in the registry.</summary>
    public required int MaxInternalChildren { get; init; }

    /// <summary>Number of independent physical shards the key space is divided into. Pinned in the registry.</summary>
    public required int ShardCount { get; init; }

    /// <summary>
    /// Every public, readable, writable, non-indexer instance property declared on
    /// the base <see cref="LatticeOptions"/> type. Enumerated once and cached.
    /// <para>
    /// This is what lets <see cref="CopyConfigurableBaseOptionsFrom"/> copy the
    /// full configuration surface without a hand-maintained assignment list. The
    /// three structural pins above (<see cref="MaxLeafKeys"/>,
    /// <see cref="MaxInternalChildren"/>, <see cref="ShardCount"/>) are declared on
    /// this derived type, not on <see cref="LatticeOptions"/>, and are
    /// <c>init</c>-only, so they are excluded on both counts and never clobbered by
    /// the copy.
    /// </para>
    /// </summary>
    private static readonly PropertyInfo[] ConfigurableBaseProperties =
        typeof(LatticeOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(static p => p.GetGetMethod(nonPublic: false) is not null
                            && p.GetSetMethod(nonPublic: false) is not null
                            && p.GetIndexParameters().Length == 0)
            .ToArray();

    /// <summary>
    /// Copies every configurable base <see cref="LatticeOptions"/> property from
    /// <paramref name="source"/> onto this instance by reflection, so no property
    /// can be silently dropped by an omission from a hand-maintained copy block
    /// (issues #2182 and #2203). Callers apply the small set of derived or
    /// registry-pinned overrides (stall-duration folding, compaction-floor clamps,
    /// projection-digest latch, WAL-partition pin, per-tree cache cap)
    /// <em>after</em> this call, so those transformed values win over the raw
    /// configured value copied here.
    /// <para>
    /// Invoked once per tree activation from
    /// <see cref="LatticeOptionsResolver.ResolveAsync"/> (the resolved options are
    /// cached per grain activation), so the reflective set is off the per-operation
    /// hot path.
    /// </para>
    /// </summary>
    internal void CopyConfigurableBaseOptionsFrom(LatticeOptions source)
    {
        ArgumentNullException.ThrowIfNull(source);
        foreach (var property in ConfigurableBaseProperties)
        {
            property.SetValue(this, property.GetValue(source));
        }
    }
}

