using System.Collections.ObjectModel;
using System.Runtime.InteropServices;
using Orleans.Lattice;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaComplianceAdmin"/>: a read-only per-tree
/// compliance audit that streams the tree's current values through the cached
/// compiled policy and tallies compliant vs non-compliant counts, grouping the
/// non-compliant population by failure reason. It reuses the enforcement hot-path
/// <see cref="ILatticeSchemaPolicyProvider"/> cache, so the audit pays no policy
/// recompilation, and enumerates values through the same
/// <see cref="ILattice.EntriesAsync"/> read seam the remediation coordinator uses.
/// </summary>
/// <remarks>
/// The scan is allocation-lean on the per-value hot path: it performs one
/// <see cref="CompiledSchemaPolicy.Validate"/> call per value, allocates the
/// reason-breakdown dictionary lazily only when the first non-compliant value is
/// seen, and accumulates reason counts through a single hashed lookup per
/// non-compliant value. No per-value LINQ, closure, or boxing is incurred.
/// </remarks>
internal sealed class LatticeSchemaComplianceAdmin(
    IGrainFactory grainFactory,
    ILatticeSchemaPolicyProvider policyProvider) : ILatticeSchemaComplianceAdmin
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly ILatticeSchemaPolicyProvider _policyProvider =
        policyProvider ?? throw new ArgumentNullException(nameof(policyProvider));

    /// <inheritdoc />
    public async Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var policy = await _policyProvider.GetCompiledPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (policy is null)
        {
            return LatticeSchemaComplianceReport.Ungoverned(treeId);
        }

        var compliant = 0;
        var nonCompliant = 0;
        Dictionary<string, int>? breakdown = null;

        var source = _grainFactory.GetGrain<ILattice>(treeId);
        await foreach (var entry in source
            .ScanEntriesAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (policy.Validate(entry.Value) is { } reason)
            {
                nonCompliant++;
                breakdown ??= new Dictionary<string, int>(StringComparer.Ordinal);
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(breakdown, reason, out _);
                slot++;
            }
            else
            {
                compliant++;
            }
        }

        return new LatticeSchemaComplianceReport
        {
            TreeId = treeId,
            HasPolicy = true,
            CompliantCount = compliant,
            NonCompliantCount = nonCompliant,
            ScannedCount = compliant + nonCompliant,
            RuleBreakdown = BuildBreakdown(breakdown),
        };
    }

    private static IReadOnlyList<LatticeSchemaComplianceRuleCount> BuildBreakdown(Dictionary<string, int>? breakdown)
    {
        if (breakdown is null || breakdown.Count == 0)
        {
            return ReadOnlyCollection<LatticeSchemaComplianceRuleCount>.Empty;
        }

        var rows = new LatticeSchemaComplianceRuleCount[breakdown.Count];
        var i = 0;
        foreach (var pair in breakdown)
        {
            rows[i++] = new LatticeSchemaComplianceRuleCount { Reason = pair.Key, Count = pair.Value };
        }

        return rows;
    }
}
