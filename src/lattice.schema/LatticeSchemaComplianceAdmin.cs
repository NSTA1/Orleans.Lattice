using System.Collections.ObjectModel;
using System.Runtime.InteropServices;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaComplianceAdmin"/>: a read-only per-tree
/// compliance audit that streams the tree's current values through the cached
/// compiled policy and tallies compliant vs non-compliant counts, grouping the
/// non-compliant population by failure reason. It reuses the enforcement hot-path
/// <see cref="ILatticeSchemaPolicyProvider"/> cache, so the audit pays no policy
/// recompilation, and enumerates values through the same
/// <c>LatticeExtensions.ScanEntriesAsync</c> read seam the remediation coordinator uses.
/// </summary>
/// <remarks>
/// <para>
/// The scan is allocation-lean on the per-value hot path: it performs one
/// <see cref="CompiledSchemaPolicy.Validate"/> call per value, allocates the
/// reason-breakdown dictionary lazily only when the first non-compliant value is
/// seen, and accumulates reason counts through a single hashed lookup per
/// non-compliant value. No per-value LINQ, closure, or boxing is incurred.
/// </para>
/// <para>
/// A value is judged as a client read serves it: on a versioning-enabled silo the
/// read-path <see cref="ILatticeValueDecoder"/> strips its envelope and upcasts it to
/// the tree's target. A stored value the decoder cannot serve at the target (no
/// upcast path, or stamped newer than the target) is <b>non-compliant</b>, under the
/// reason text enforcement uses for it (<see cref="SchemaUndecodableValue"/>), and
/// the scan completes (issue #5253). The client-facing entry scan decodes inside the
/// tree and throws on such a value, so on the first one the scan finishes the rest of
/// the tree from the stored form instead: keys from the same access-gated key scan,
/// each value point-read with its envelope intact and decoded here, per value, by the
/// same decoder, so a value that cannot be decoded fails on its own key. A tree
/// holding no undecodable value never leaves the streaming scan.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaComplianceAdmin(
    IGrainFactory grainFactory,
    ILatticeSchemaPolicyProvider policyProvider,
    ILatticeValueDecoder? valueDecoder = null) : ILatticeSchemaComplianceAdmin
{
    /// <summary>
    /// How many keys the stored-form tail point-reads concurrently. Bounds the
    /// in-flight reads and the per-page key list.
    /// </summary>
    private const int StoredFormPageSize = 256;

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

        var tally = new Tally();

        var source = _grainFactory.GetGrain<ILattice>(treeId);

        // Tracked-operation progress (#4126): null outside a tracked operation, so
        // the blocking scan pays one null check and never counts the tree.
        var progress = LatticeOperationProgress.Current;
        long? total = null;
        if (progress is not null)
        {
            await progress.ReportAsync(SchemaComplianceScanOperation.CountingPhase).ConfigureAwait(false);
            total = await source.CountAsync(cancellationToken).ConfigureAwait(false);
            await progress.ReportAsync(
                SchemaComplianceScanOperation.ScanningPhase, 0, total, SchemaComplianceScanOperation.EntriesUnit)
                .ConfigureAwait(false);
        }

        string? lastKey = null;
        var undecodable = false;
        try
        {
            await foreach (var entry in source
                .ScanEntriesAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                tally.Count(policy.Validate(entry.Value));
                lastKey = entry.Key;

                if (progress is not null && (tally.Scanned & (ProgressReportInterval - 1)) == 0)
                {
                    total = await ReportScannedAsync(progress, tally.Scanned, total).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (SchemaUndecodableValue.IsDecodeFailure(ex) && DecoderActive(treeId))
        {
            undecodable = true;
        }

        if (undecodable)
        {
            total = await ScanStoredFormAsync(source, treeId, policy, tally, lastKey, progress, total, cancellationToken)
                .ConfigureAwait(false);
        }

        if (progress is not null)
        {
            await ReportScannedAsync(progress, tally.Scanned, total).ConfigureAwait(false);
        }

        return new LatticeSchemaComplianceReport
        {
            TreeId = treeId,
            HasPolicy = true,
            RuleCount = policy.RuleCount,
            CompliantCount = tally.Compliant,
            NonCompliantCount = tally.NonCompliant,
            ScannedCount = tally.Scanned,
            RuleBreakdown = BuildBreakdown(tally.Breakdown),
        };
    }

    /// <summary>
    /// How many scanned values pass between progress reports (a power of two). The
    /// sink coalesces reports anyway; this keeps the per-value cost to a mask test.
    /// </summary>
    private const int ProgressReportInterval = 256;

    private bool DecoderActive(string treeId) => valueDecoder is { } decoder && decoder.IsActive(treeId);

    /// <summary>
    /// Judges every value after <paramref name="lastKey"/> from its stored form: keys
    /// from the access-gated key scan, values point-read with their envelope intact
    /// and decoded here, one at a time, so a value the decoder cannot serve at the
    /// target is counted non-compliant on its own key instead of ending the scan.
    /// </summary>
    private async Task<long?> ScanStoredFormAsync(
        ILattice source,
        string treeId,
        CompiledSchemaPolicy policy,
        Tally tally,
        string? lastKey,
        ILatticeOperationProgress? progress,
        long? total,
        CancellationToken cancellationToken)
    {
        var reader = _grainFactory.GetGrain<IReplicationApplyGrain>(treeId);
        var keys = new List<string>(StoredFormPageSize);
        var start = lastKey is null ? null : lastKey + '\0';
        await foreach (var key in source
            .ScanKeysAsync(startInclusive: start, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            keys.Add(key);
            if (keys.Count == StoredFormPageSize)
            {
                total = await JudgeStoredPageAsync(reader, treeId, policy, tally, keys, progress, total, cancellationToken)
                    .ConfigureAwait(false);
                keys.Clear();
            }
        }

        if (keys.Count > 0)
        {
            total = await JudgeStoredPageAsync(reader, treeId, policy, tally, keys, progress, total, cancellationToken)
                .ConfigureAwait(false);
        }

        return total;
    }

    private async Task<long?> JudgeStoredPageAsync(
        IReplicationApplyGrain reader,
        string treeId,
        CompiledSchemaPolicy policy,
        Tally tally,
        List<string> keys,
        ILatticeOperationProgress? progress,
        long? total,
        CancellationToken cancellationToken)
    {
        var reads = new Task<VersionedValue>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            reads[i] = reader.ReadStoredWithVersionAsync(keys[i]);
        }

        await Task.WhenAll(reads).ConfigureAwait(false);
        var decoder = valueDecoder!;
        for (var i = 0; i < reads.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // A key deleted since the key scan reached it is no longer a current value.
            if (reads[i].Result.Value is not { } stored)
            {
                continue;
            }

            string? reason;
            try
            {
                var judged = await decoder.DecodeAsync(treeId, stored, cancellationToken).ConfigureAwait(false);
                reason = policy.Validate(judged);
            }
            catch (Exception ex) when (SchemaUndecodableValue.IsDecodeFailure(ex))
            {
                reason = SchemaUndecodableValue.Reason(ex);
            }

            tally.Count(reason);
            if (progress is not null && (tally.Scanned & (ProgressReportInterval - 1)) == 0)
            {
                total = await ReportScannedAsync(progress, tally.Scanned, total).ConfigureAwait(false);
            }
        }

        return total;
    }

    private static async ValueTask<long?> ReportScannedAsync(ILatticeOperationProgress progress, long scanned, long? total)
    {
        // The count was taken before the scan over a writable tree, so a scan can
        // pass it. A total is never reported below the entries already scanned:
        // once passed it becomes unknown rather than wrong.
        if (total is { } known && scanned > known)
        {
            total = null;
        }

        await progress.ReportAsync(
            SchemaComplianceScanOperation.ScanningPhase, scanned, total, SchemaComplianceScanOperation.EntriesUnit)
            .ConfigureAwait(false);
        return total;
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

    /// <summary>
    /// The running counts of one scan, shared by the streaming scan and the
    /// stored-form tail. One instance per scan; the breakdown is allocated only when
    /// the first non-compliant value is seen.
    /// </summary>
    private sealed class Tally
    {
        public int Compliant;

        public int NonCompliant;

        public Dictionary<string, int>? Breakdown;

        public int Scanned => Compliant + NonCompliant;

        public void Count(string? reason)
        {
            if (reason is null)
            {
                Compliant++;
                return;
            }

            NonCompliant++;
            Breakdown ??= new Dictionary<string, int>(StringComparer.Ordinal);
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Breakdown, reason, out _);
            slot++;
        }
    }
}
