using MultiSiteManufacturing.Host.Domain;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Custom folded-aggregation projection that materialises each part's
/// <see cref="ComplianceState"/> as a grouped fold over the <c>mfg-facts</c>
/// tree, so a state read is an O(1) lookup of one pre-folded row instead of a
/// per-part prefix scan that re-folds every fact on every read (the work
/// <see cref="ComplianceFold"/> does inline in <see cref="LatticeFactBackend"/>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why not the built-in <c>LatticeFoldProjection.Create</c>.</b> The built-in
/// fold orders each group's members by the source <em>write</em> HLC (the WAL
/// commit timestamp of the <c>Set</c>). This sample instead orders facts by the
/// <em>business</em> <see cref="Fact.Hlc"/> assigned at the origin site, exactly
/// like <see cref="ComplianceFold"/>: a late-arriving fact with an earlier
/// business HLC must fold into its correct logical position regardless of arrival
/// order. To reproduce that, <see cref="Project"/> lowers a <c>Set</c> to a
/// <see cref="AggregationContribution.Fold(string, string, byte[], HybridLogicalClock)"/>
/// carrying <see cref="Fact.Hlc"/> as the ordering timestamp, so the maintainer
/// (which re-folds surviving members in ascending contribution-HLC order) applies
/// the group's facts in business-HLC order.
/// </para>
/// <para>
/// <b>Group key.</b> Each fact contributes to the group named by its
/// <see cref="Fact.Serial"/>, so the materialised row is stored under the bare
/// serial and read with <c>view.GetAsync&lt;ComplianceAccumulator&gt;(serial)</c>.
/// </para>
/// <para>
/// <b>Ordering caveat.</b> The maintainer breaks an exact HLC tie by the source
/// key (<c>{serial}/{wallTicks:D20}/{counter:D10}/{factId:N}</c>) compared
/// ordinally, whereas <see cref="ComplianceFold"/> breaks the same tie by
/// <see cref="Fact.FactId"/> compared as a <see cref="Guid"/>. The two tiebreaks
/// can differ only for two facts sharing an identical wall-clock tick <em>and</em>
/// counter; the sample never emits such a pair (its fact HLCs use distinct ticks),
/// so the materialised state matches the inline fold in every exercised scenario.
/// The fold logic itself (<see cref="StateTransitions.Apply"/>) is shared, so the
/// two paths cannot diverge on the transition rules.
/// </para>
/// </remarks>
public sealed class ComplianceFoldProjection : ILatticeFoldProjection
{
    /// <summary>Logical view name for the materialised per-part compliance state.</summary>
    public const string ViewName = "mfg-compliance";

    /// <summary>
    /// Stable tag identifying this fold's logic. Bump it whenever
    /// <see cref="StateTransitions.Apply"/>, the accumulator shape, or the
    /// member ordering changes, so the maintainer rebuilds the view.
    /// </summary>
    private const string FoldVersionTag = "compliance-fold-v1";

    private static readonly ILatticeSerializer<ComplianceAccumulator> AccumulatorSerializer =
        JsonLatticeSerializer<ComplianceAccumulator>.Default;

    /// <inheritdoc />
    public AggregationKind Aggregation => AggregationKind.Fold;

    /// <inheritdoc />
    public string ProjectionVersion => FoldVersionTag;

    /// <inheritdoc />
    public byte[] Initial() => AccumulatorSerializer.Serialize(ComplianceAccumulator.Seed);

    /// <inheritdoc />
    public byte[] Apply(byte[] accumulator, string sourceKey, byte[] sourceValue, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(accumulator);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(sourceValue);

        var current = AccumulatorSerializer.Deserialize(accumulator);
        var fact = FactJsonCodec.Decode(sourceValue);
        var (state, retestArmed) = StateTransitions.Apply(current.State, current.RetestArmed, fact);

        // Members are applied in ascending business-HLC order, so each Apply
        // sees a newer fact than the last: overwriting LatestStage every call
        // leaves the newest fact's stage, matching ComplianceFold's "stage of
        // the latest fact". FactCount grows by one per surviving member; the
        // maintainer re-folds the whole group from Initial() on any retraction,
        // so the per-Apply increment stays exact.
        return AccumulatorSerializer.Serialize(new ComplianceAccumulator(
            state,
            retestArmed,
            ProcessStageMap.Of(fact),
            current.FactCount + 1));
    }

    /// <inheritdoc />
    public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.Value is null)
                {
                    yield break;
                }

                // Order the fold by the business HLC assigned at the origin site
                // (not the write HLC), matching ComplianceFold's ordering so a
                // late-arriving earlier fact still folds into its logical position.
                var fact = FactJsonCodec.Decode(mutation.Value);
                yield return AggregationContribution.Fold(
                    fact.Serial.Value,
                    mutation.Key,
                    mutation.Value,
                    fact.Hlc);
                break;

            case MutationKind.Delete:
            case MutationKind.Tombstone:
                yield return AggregationContribution.Retract(mutation.Key, mutation.Timestamp);
                break;

            case MutationKind.DeleteRange:
                if (mutation.MatchedKeys is { Count: > 0 } matched)
                {
                    foreach (var key in matched)
                    {
                        yield return AggregationContribution.Retract(key, mutation.Timestamp);
                    }

                    yield break;
                }

                if (!string.IsNullOrEmpty(mutation.EndExclusiveKey))
                {
                    yield return AggregationContribution.RangeReconcile(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                }

                break;

            default:
                yield break;
        }
    }
}

/// <summary>
/// The fold accumulator the <see cref="ComplianceFoldProjection"/> materialises
/// per part: the running <see cref="ComplianceState"/> and the retest-armed flag
/// that <see cref="StateTransitions.Apply"/> threads through the fold, plus the
/// fact-derived summary fields the dashboard needs (the newest fact's
/// <see cref="ProcessStage"/> and the running fact count). Serialised as the
/// view's opaque group value and read back with
/// <c>view.GetAsync&lt;ComplianceAccumulator&gt;(serial)</c>.
/// </summary>
/// <param name="State">The folded compliance state.</param>
/// <param name="RetestArmed">Whether a prior retest pass armed the demotion flag.</param>
/// <param name="LatestStage">The <see cref="ProcessStage"/> of the newest folded fact, or <c>null</c> before any fact is applied.</param>
/// <param name="FactCount">The number of facts folded into this accumulator.</param>
public sealed record ComplianceAccumulator(
    ComplianceState State,
    bool RetestArmed,
    ProcessStage? LatestStage,
    int FactCount)
{
    /// <summary>The empty accumulator: a Nominal part with no armed retest, no stage, and no facts.</summary>
    public static ComplianceAccumulator Seed { get; } = new(ComplianceState.Nominal, false, null, 0);
}
