using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three reductions made to the <i>steady-state reconcile</i> and
/// <i>sequential replication apply</i> paths, so the per-operation time and byte
/// deltas are measurable in the clear.
/// <para>
/// Each pair runs the prior shape against its replacement with no silo, no
/// transport and no filesystem in the loop, so the delta is precisely the work
/// the production change removes. Both lanes of a pair reproduce the <b>same</b>
/// surrounding shell - identical inputs, identical interface-typed access,
/// identical item construction and identical emit logic - and differ only in the
/// body under test, because a baseline that skips part of the optimized arm's
/// shell fabricates a regression (see the <c>ReceiverAppliedContentIndex</c> note
/// in <see cref="ReplicationShipApplyTrimBenchmarks"/>).
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the run-segmentation scan in
/// <c>ReplicationApplier.ApplyRunsSequentiallyAsync</c>, which is the
/// <b>default</b> apply path (a single-tree batch never reaches the parallel
/// planner, and <c>ApplyMaxParallelRuns</c> defaults to <c>1</c>).
/// <see cref="WalRecord"/> is a wide <c>readonly record struct</c>, so every read
/// through the <see cref="IReadOnlyList{T}"/> indexer copies the whole record
/// onto the stack; the prior shape indexed the candidate <b>three</b> times per
/// entry (tree, origin, mode). The replacement binds each candidate once in a
/// synchronous helper, which also keeps the wide struct out of the caller's async
/// state machine;
/// (2) <c>RepoTreeWalker.GroupKnownByDirectory</c> and
/// <c>GroupDirectoriesByParent</c>, which run on every reconcile pass. The prior
/// shape allocated a throwaway parent substring for <b>every file</b> and grew a
/// <see cref="List{T}"/> per directory from empty. The replacement keys the
/// buckets on the parent <b>span</b> through a
/// <see cref="Dictionary{TKey, TValue}.AlternateLookup{TAlternateKey}"/> - one
/// materialised parent string per <i>directory</i> - and fills one exact-width
/// array per bucket via count-then-fill;
/// (3) <c>RepoContextBootstrapPlan.Compute</c>, the reconcile diff. It grew four
/// <see cref="List{T}"/>s of a ~40-byte <c>readonly record struct</c> from empty,
/// so an unchanged tree walked the whole 4/8/16/.../8192 doubling chain and
/// abandoned every intermediate backing array. The replacement classifies into a
/// one-byte-per-file side buffer, then fills one exact-width array per class -
/// and an empty class allocates nothing at all.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=ingestapplytrims</c> (or
/// <c>--suite ingestapplytrims</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class IngestPlanAndSequentialApplyBenchmarks
{
    // ---- (1) a single-tree, single-origin inbound batch: the steady-state
    //      shape, and the only one the sequential apply path sees, since the
    //      transport ships per-(tree, peer). Held behind the interface so both
    //      lanes pay the same indexer dispatch the production code pays ----
    private IReadOnlyList<WalRecord> _sequentialBatch = null!;

    // ---- (2) the stored file facts and the prior directory snapshot a pruned
    //      reconcile pass groups, at the scale of this repository ----
    private Dictionary<string, BenchStoredFileMeta> _knownFiles = null!;
    private Dictionary<string, long> _previousDirectoryMtimes = null!;

    // ---- (3) a fresh scan of an unchanged tree plus its stored digests: the
    //      steady state, where nearly every file classifies as unchanged ----
    private BenchRepoFileEntry[] _scanned = null!;
    private Dictionary<string, string> _storedDigests = null!;

    private const int BatchLength = 512;
    private const int DirectoryCount = 512;
    private const int FilesPerDirectory = 8;
    private const int FileCount = DirectoryCount * FilesPerDirectory;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var payload = new byte[128];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)i;
        }

        // (1) one long same-origin, same-tree run - the shape that makes the
        // inner scan's per-candidate struct copies dominate.
        var batch = new WalRecord[BatchLength];
        for (var i = 0; i < BatchLength; i++)
        {
            batch[i] = new WalRecord
            {
                TreeId = "orders",
                Op = MutationKind.Set,
                Key = "customer/" + i.ToString("D6"),
                Value = payload,
                Timestamp = new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + i,
                    Counter = 0,
                },
                OriginClusterId = "region-00",
                Mode = LatticeMergeMode.LwwRegister,
            };
        }
        _sequentialBatch = batch;

        // (2) + (3) a tree with 512 directories of 8 files each, nested three
        // levels deep so the parent slice is a realistic length rather than a
        // single segment.
        _knownFiles = new Dictionary<string, BenchStoredFileMeta>(FileCount, StringComparer.Ordinal);
        _previousDirectoryMtimes = new Dictionary<string, long>(DirectoryCount + 1, StringComparer.Ordinal);
        _storedDigests = new Dictionary<string, string>(FileCount, StringComparer.Ordinal);
        _scanned = new BenchRepoFileEntry[FileCount];
        _previousDirectoryMtimes[string.Empty] = 1;

        var slot = 0;
        for (var d = 0; d < DirectoryCount; d++)
        {
            var directory = "src/package-" + (d / 32).ToString("D2")
                + "/area-" + (d % 32).ToString("D2")
                + "/component";
            _previousDirectoryMtimes[directory] = 638_000_000_000_000_000L + d;

            for (var f = 0; f < FilesPerDirectory; f++)
            {
                var relativePath = directory + "/File" + f.ToString("D2") + ".cs";
                var digest = "sha256:" + (d * FilesPerDirectory + f).ToString("D16");
                _knownFiles[relativePath] = new BenchStoredFileMeta(
                    digest, "csharp", 4096, 638_000_000_000_000_000L, Array.Empty<string>());
                _storedDigests[relativePath] = digest;
                _scanned[slot++] = new BenchRepoFileEntry(relativePath, digest, 4096, "csharp");
            }
        }
    }

    // ========================================================================
    // (1) run segmentation on the sequential apply path
    // ========================================================================

    /// <summary>
    /// The prior shape: the inner run scan read the candidate record through the
    /// interface indexer three times per entry (tree, origin, mode), copying the
    /// whole wide struct each time, and the run head three more times.
    /// </summary>
    [Benchmark]
    public int SequentialRunScan_Baseline_TripleIndexPerCandidate()
    {
        var entries = _sequentialBatch;
        var runs = 0;
        var span = 0;
        var i = 0;
        while (i < entries.Count)
        {
            var startTreeId = entries[i].TreeId;
            var startOrigin = entries[i].OriginClusterId;
            var startMode = entries[i].Mode;
            var j = i + 1;
            while (j < entries.Count
                && string.Equals(entries[j].TreeId, startTreeId, StringComparison.Ordinal)
                && string.Equals(entries[j].OriginClusterId, startOrigin, StringComparison.Ordinal)
                && entries[j].Mode == startMode)
            {
                j++;
            }

            runs++;
            span += j - i;
            i = j;
        }

        return runs + span;
    }

    /// <summary>
    /// The shipped shape: the head and each candidate are bound once and the
    /// run-key fields are projected off that single copy, so one struct copy per
    /// entry replaces three.
    /// </summary>
    [Benchmark]
    public int SequentialRunScan_Optimized_HoistedRead()
    {
        var entries = _sequentialBatch;
        var runs = 0;
        var span = 0;
        var i = 0;
        while (i < entries.Count)
        {
            var j = FindRunEndExclusive(entries, i);
            runs++;
            span += j - i;
            i = j;
        }

        return runs + span;
    }

    // A verbatim copy of the shipped private helper, so the lane measures the
    // production body rather than an approximation of it.
    private static int FindRunEndExclusive(IReadOnlyList<WalRecord> entries, int start)
    {
        var head = entries[start];
        var startTreeId = head.TreeId;
        var startOrigin = head.OriginClusterId;
        var startMode = head.Mode;

        var j = start + 1;
        while (j < entries.Count)
        {
            var candidate = entries[j];
            if (!string.Equals(candidate.TreeId, startTreeId, StringComparison.Ordinal)
                || !string.Equals(candidate.OriginClusterId, startOrigin, StringComparison.Ordinal)
                || candidate.Mode != startMode)
            {
                break;
            }

            j++;
        }

        return j;
    }

    // ========================================================================
    // (2) per-directory grouping on the pruned reconcile walk
    // ========================================================================

    /// <summary>
    /// The prior shape: a parent substring allocated for every file and every
    /// directory, and a <see cref="List{T}"/> per bucket grown from empty.
    /// </summary>
    [Benchmark]
    public int DirectoryGrouping_Baseline_ListBucketsAndPerItemSubstring()
    {
        var byDirectory = new Dictionary<string, List<(string Relative, BenchStoredFileMeta Meta)>>(
            StringComparer.Ordinal);
        foreach (var (relativePath, meta) in _knownFiles)
        {
            var parent = ParentDirectory(relativePath);
            if (!byDirectory.TryGetValue(parent, out var files))
            {
                files = [];
                byDirectory[parent] = files;
            }

            files.Add((relativePath, meta));
        }

        var byParent = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var relativeDir in _previousDirectoryMtimes.Keys)
        {
            if (relativeDir.Length == 0)
            {
                continue;
            }

            var parent = ParentDirectory(relativeDir);
            if (!byParent.TryGetValue(parent, out var children))
            {
                children = [];
                byParent[parent] = children;
            }

            children.Add(relativeDir);
        }

        return byDirectory.Count + byParent.Count;
    }

    /// <summary>
    /// The shipped shape: span-keyed alternate lookups materialise one parent
    /// string per directory rather than per item, and count-then-fill gives every
    /// bucket exactly one allocation at its exact final width.
    /// </summary>
    [Benchmark]
    public int DirectoryGrouping_Optimized_SpanKeyedExactWidthBuckets()
    {
        var byDirectory = GroupKnownByDirectory(_knownFiles);
        var byParent = GroupDirectoriesByParent(_previousDirectoryMtimes);
        return byDirectory.Count + byParent.Count;
    }

    /// <summary>
    /// A contrast arm: the span-keyed substring elision on its own, keeping the
    /// list buckets. One probe per item instead of the two count-then-fill needs,
    /// but the bucket growth chain stays. Measured to decide whether the second
    /// pass earns its extra probe.
    /// </summary>
    [Benchmark]
    public int DirectoryGrouping_Contrast_SpanKeyedListBucketsSinglePass()
    {
        var byDirectory = new Dictionary<string, List<(string Relative, BenchStoredFileMeta Meta)>>(
            StringComparer.Ordinal);
        var dirLookup = byDirectory.GetAlternateLookup<ReadOnlySpan<char>>();
        foreach (var (relativePath, meta) in _knownFiles)
        {
            var parent = ParentDirectorySpan(relativePath);
            ref var files = ref CollectionsMarshal.GetValueRefOrNullRef(dirLookup, parent);
            if (Unsafe.IsNullRef(ref files))
            {
                byDirectory[parent.ToString()] = [(relativePath, meta)];
            }
            else
            {
                files.Add((relativePath, meta));
            }
        }

        var byParent = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var parentLookup = byParent.GetAlternateLookup<ReadOnlySpan<char>>();
        foreach (var relativeDir in _previousDirectoryMtimes.Keys)
        {
            if (relativeDir.Length == 0)
            {
                continue;
            }

            var parent = ParentDirectorySpan(relativeDir);
            ref var children = ref CollectionsMarshal.GetValueRefOrNullRef(parentLookup, parent);
            if (Unsafe.IsNullRef(ref children))
            {
                byParent[parent.ToString()] = [relativeDir];
            }
            else
            {
                children.Add(relativeDir);
            }
        }

        return byDirectory.Count + byParent.Count;
    }

    // Verbatim copies of the shipped private helpers.
    private static Dictionary<string, (string Relative, BenchStoredFileMeta Meta)[]> GroupKnownByDirectory(
        IReadOnlyDictionary<string, BenchStoredFileMeta> knownFiles)
    {
        var buckets = new Dictionary<string, (int Cursor, (string Relative, BenchStoredFileMeta Meta)[] Items)>(
            StringComparer.Ordinal);
        var lookup = buckets.GetAlternateLookup<ReadOnlySpan<char>>();

        foreach (var relativePath in knownFiles.Keys)
        {
            var parent = ParentDirectorySpan(relativePath);
            ref var cell = ref CollectionsMarshal.GetValueRefOrNullRef(lookup, parent);
            if (Unsafe.IsNullRef(ref cell))
            {
                buckets[parent.ToString()] = (1, []);
            }
            else
            {
                cell.Cursor++;
            }
        }

        foreach (var (relativePath, meta) in knownFiles)
        {
            ref var cell = ref CollectionsMarshal.GetValueRefOrNullRef(
                lookup, ParentDirectorySpan(relativePath));

            if (cell.Items.Length == 0)
            {
                cell.Items = new (string Relative, BenchStoredFileMeta Meta)[cell.Cursor];
                cell.Cursor = 0;
            }

            cell.Items[cell.Cursor++] = (relativePath, meta);
        }

        var byDirectory = new Dictionary<string, (string Relative, BenchStoredFileMeta Meta)[]>(
            buckets.Count,
            StringComparer.Ordinal);
        foreach (var (directory, cell) in buckets)
        {
            byDirectory[directory] = cell.Items;
        }

        return byDirectory;
    }

    private static Dictionary<string, string[]> GroupDirectoriesByParent(
        IReadOnlyDictionary<string, long> previousDirectoryMtimes)
    {
        var buckets = new Dictionary<string, (int Cursor, string[] Items)>(StringComparer.Ordinal);
        var lookup = buckets.GetAlternateLookup<ReadOnlySpan<char>>();

        foreach (var relativeDir in previousDirectoryMtimes.Keys)
        {
            if (relativeDir.Length == 0)
            {
                continue;
            }

            var parent = ParentDirectorySpan(relativeDir);
            ref var cell = ref CollectionsMarshal.GetValueRefOrNullRef(lookup, parent);
            if (Unsafe.IsNullRef(ref cell))
            {
                buckets[parent.ToString()] = (1, []);
            }
            else
            {
                cell.Cursor++;
            }
        }

        foreach (var relativeDir in previousDirectoryMtimes.Keys)
        {
            if (relativeDir.Length == 0)
            {
                continue;
            }

            ref var cell = ref CollectionsMarshal.GetValueRefOrNullRef(
                lookup, ParentDirectorySpan(relativeDir));

            if (cell.Items.Length == 0)
            {
                cell.Items = new string[cell.Cursor];
                cell.Cursor = 0;
            }

            cell.Items[cell.Cursor++] = relativeDir;
        }

        var byParent = new Dictionary<string, string[]>(buckets.Count, StringComparer.Ordinal);
        foreach (var (parent, cell) in buckets)
        {
            byParent[parent] = cell.Items;
        }

        return byParent;
    }
    private static string ParentDirectory(string relativePath)
    {
        var lastSlash = relativePath.LastIndexOf('/');
        return lastSlash < 0 ? string.Empty : relativePath[..lastSlash];
    }

    private static ReadOnlySpan<char> ParentDirectorySpan(string relativePath)
    {
        var lastSlash = relativePath.LastIndexOf('/');
        return lastSlash < 0 ? [] : relativePath.AsSpan(0, lastSlash);
    }

    // ========================================================================
    // (3) the reconcile diff in RepoContextBootstrapPlan.Compute
    // ========================================================================

    /// <summary>
    /// The prior shape: four <see cref="List{T}"/>s of a wide
    /// <c>readonly record struct</c> grown from empty, plus a prune list
    /// allocated even when the tree is unchanged and nothing is pruned.
    /// </summary>
    [Benchmark]
    public int BootstrapPlanCompute_Baseline_FourListsGrownFromEmpty()
    {
        var scanned = _scanned;
        var storedDigests = _storedDigests;

        var added = new List<BenchRepoFileEntry>();
        var updated = new List<BenchRepoFileEntry>();
        var unchanged = new List<BenchRepoFileEntry>();
        var metadataChanged = new List<BenchRepoFileEntry>();
        var scannedPaths = new HashSet<string>(scanned.Length, StringComparer.Ordinal);

        foreach (var entry in scanned)
        {
            scannedPaths.Add(entry.RelativePath);
            if (!storedDigests.TryGetValue(entry.RelativePath, out var storedDigest))
            {
                added.Add(entry);
            }
            else if (!string.Equals(storedDigest, entry.Digest, StringComparison.Ordinal))
            {
                updated.Add(entry);
            }
            else if (entry.AnchorStale)
            {
                metadataChanged.Add(entry);
            }
            else
            {
                unchanged.Add(entry);
            }
        }

        var removed = new List<string>();
        foreach (var storedPath in storedDigests.Keys)
        {
            if (!scannedPaths.Contains(storedPath))
            {
                removed.Add(storedPath);
            }
        }

        removed.Sort(StringComparer.Ordinal);
        return added.Count + updated.Count + unchanged.Count + metadataChanged.Count + removed.Count;
    }

    /// <summary>
    /// The shipped shape: one classification pass into a one-byte-per-file side
    /// buffer, then one exact-width array per class. An empty class allocates
    /// nothing, and the digest map is still probed exactly once per file.
    /// </summary>
    [Benchmark]
    public int BootstrapPlanCompute_Optimized_CountThenFill()
    {
        var scanned = _scanned;
        var storedDigests = _storedDigests;

        var scannedCount = scanned.Length;
        byte[] classes = scannedCount == 0 ? [] : new byte[scannedCount];
        var scannedPaths = new HashSet<string>(scannedCount, StringComparer.Ordinal);
        Span<int> widths = stackalloc int[ClassCount];

        for (var i = 0; i < scannedCount; i++)
        {
            var entry = scanned[i];
            scannedPaths.Add(entry.RelativePath);
            byte classification;
            if (!storedDigests.TryGetValue(entry.RelativePath, out var storedDigest))
            {
                classification = ClassAdded;
            }
            else if (!string.Equals(storedDigest, entry.Digest, StringComparison.Ordinal))
            {
                classification = ClassUpdated;
            }
            else if (entry.AnchorStale)
            {
                classification = ClassMetadataChanged;
            }
            else
            {
                classification = ClassUnchanged;
            }

            classes[i] = classification;
            widths[classification]++;
        }

        BenchRepoFileEntry[] added = widths[ClassAdded] == 0 ? [] : new BenchRepoFileEntry[widths[ClassAdded]];
        BenchRepoFileEntry[] updated = widths[ClassUpdated] == 0 ? [] : new BenchRepoFileEntry[widths[ClassUpdated]];
        BenchRepoFileEntry[] unchanged = widths[ClassUnchanged] == 0
            ? []
            : new BenchRepoFileEntry[widths[ClassUnchanged]];
        BenchRepoFileEntry[] metadataChanged = widths[ClassMetadataChanged] == 0
            ? []
            : new BenchRepoFileEntry[widths[ClassMetadataChanged]];

        Span<int> cursors = stackalloc int[ClassCount];
        for (var i = 0; i < scannedCount; i++)
        {
            var classification = classes[i];
            var slot = cursors[classification]++;
            switch (classification)
            {
                case ClassAdded:
                    added[slot] = scanned[i];
                    break;
                case ClassUpdated:
                    updated[slot] = scanned[i];
                    break;
                case ClassMetadataChanged:
                    metadataChanged[slot] = scanned[i];
                    break;
                default:
                    unchanged[slot] = scanned[i];
                    break;
            }
        }

        List<string>? removed = null;
        foreach (var storedPath in storedDigests.Keys)
        {
            if (!scannedPaths.Contains(storedPath))
            {
                removed ??= [];
                removed.Add(storedPath);
            }
        }

        removed?.Sort(StringComparer.Ordinal);
        return added.Length + updated.Length + unchanged.Length + metadataChanged.Length
            + (removed?.Count ?? 0);
    }

    private const byte ClassUnchanged = 0;
    private const byte ClassAdded = 1;
    private const byte ClassUpdated = 2;
    private const byte ClassMetadataChanged = 3;
    private const int ClassCount = 4;

    /// <summary>
    /// A faithful copy of the internal <c>StoredFileMeta</c> record, so both lanes
    /// move a value of the same width through their buckets.
    /// </summary>
    public readonly record struct BenchStoredFileMeta(
        string Digest,
        string Language,
        long SizeBytes,
        long IngestWallTicks,
        IReadOnlyList<string> DeclaredSymbols,
        bool SymbolsProcessed = false,
        bool ContentProcessed = false,
        long TokenCount = -1,
        bool CrossReferenced = false);

    /// <summary>
    /// A faithful copy of the internal <c>RepoFileEntry</c> record, so both lanes
    /// move a value of the same width through their class buckets.
    /// </summary>
    public readonly record struct BenchRepoFileEntry(
        string RelativePath,
        string Digest,
        long SizeBytes,
        string Language)
    {
        public bool AnchorStale { get; init; }
    }
}
