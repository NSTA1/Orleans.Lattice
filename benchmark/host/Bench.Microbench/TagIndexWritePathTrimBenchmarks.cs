using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three allocation trims this suite was added for. All three sit
/// on <c>LatticeTagIndexContext</c>'s tag-write path, and all three are charged
/// once per tag-carrying write (or per tag query), so the per-operation delta
/// each lane reports is the heap the production change actually removes.
/// <para>
/// (1) <c>NormalizeTags</c> - every <c>WithAllTags</c> / <c>WithAnyTags</c> /
/// <c>SetValueWithTags</c> call normalised its <c>params string[]</c> by
/// accumulating into a <see cref="List{T}"/> and then returning
/// <c>list.ToArray()</c>. That is a double materialisation at an exactly known
/// capacity: the list header, the list's backing array, and then a second array
/// copied out of it. The result is at most one entry per input, so the
/// exactly-sized array can be allocated once up front and filled in place; only
/// a tag set that actually carried a duplicate pays a second, shorter array.
/// </para>
/// <para>
/// (2) Row-key construction - <c>RowKey</c>, <c>KeyRowKey</c> and
/// <c>KeyMajorPrefixFor</c> built their keys with a five- or six-operand
/// <see cref="string.Concat(string[])"/>. <see cref="string"/> only has
/// non-array <c>Concat</c> overloads up to four operands, so each of those calls
/// fell through to the <c>params string[]</c> overload and allocated a throwaway
/// array purely to describe the operands. The lengths are all known, so
/// <see cref="string.Create{TState}"/> writes the segments straight into the
/// final string and the array disappears. A write stamps one tag-major row key
/// and one key-major mirror row key <b>per tag</b>, so this is charged twice per
/// tag.
/// </para>
/// <para>
/// (3) <c>ReconcileTagSet</c>'s desired side - the method already answers
/// membership of <c>current</c> with a linear ordinal scan below
/// <c>LinearTagScanThreshold</c>, and its own remarks explain why: at a key's
/// real tag width the scan beats a set outright, because the set costs a bucket
/// array, an entry array and a hash per probe to answer a handful of
/// comparisons. The <c>desired</c> side was not given the same treatment - its
/// caller built a <see cref="HashSet{T}"/> unconditionally. These lanes close
/// that asymmetry and charge the difference.
/// </para>
/// <para>
/// Both arms of every pair are <c>NoInlining</c> statics invoked through the
/// same shell, so neither lane is flattered by an inlining decision the other
/// did not get, and the <c>TagCount</c> parameter straddles the threshold
/// (<c>4</c> is the common case and takes the linear path; <c>24</c> is the
/// above-threshold control and takes the set path, where trims (1) and (3)
/// deliberately change nothing). The baseline arms reproduce the shipped
/// pre-trim code verbatim, because the shipped method is now the optimised one.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=tagwritetrims</c> (or
/// <c>--suite tagwritetrims</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TagIndexWritePathTrimBenchmarks
{
    private const char Sep = '\0';
    private const string SepStr = "\0";
    private const string KeyMajorPrefix = "\0k\0";

    /// <summary>Mirrors <c>LatticeTagIndexContext.TagLinearDedupThreshold</c>.</summary>
    private const int TagLinearDedupThreshold = 16;

    /// <summary>Mirrors <c>LatticeTagIndexContext.LinearTagScanThreshold</c>.</summary>
    private const int LinearTagScanThreshold = 16;

    private const string TreeId = "orders-tree";
    private const string Key = "key-0007";

    /// <summary>
    /// Tag-set width. <c>4</c> is the common case and sits below both
    /// thresholds; <c>24</c> is the above-threshold control, where trims (1)
    /// and (3) take the unchanged hash-set path and must show no delta.
    /// </summary>
    [Params(4, 24)]
    public int TagCount { get; set; }

    private string[] _tags = null!;
    private string[] _tagsWithDuplicates = null!;
    private string[] _current = null!;

    [GlobalSetup]
    public void Setup()
    {
        _tags = new string[TagCount];
        for (var i = 0; i < TagCount; i++)
        {
            _tags[i] = "tag-" + i.ToString("D2", CultureInfo.InvariantCulture);
        }

        // Same width, but every other entry repeats its predecessor, so the
        // optimised arm is forced onto its resize path and the two arms can be
        // compared where they are least alike.
        _tagsWithDuplicates = new string[TagCount];
        for (var i = 0; i < TagCount; i++)
        {
            _tagsWithDuplicates[i] = "tag-" + (i - (i % 2)).ToString("D2", CultureInfo.InvariantCulture);
        }

        // The key's stored tags. Overlapping but not identical, so the
        // reconcile lanes exercise both the add and the remove partition
        // rather than converging on the empty-diff fast path.
        _current = new string[TagCount];
        for (var i = 0; i < TagCount; i++)
        {
            _current[i] = "tag-" + (i + (TagCount / 2)).ToString("D2", CultureInfo.InvariantCulture);
        }

        AssertEquivalence();
    }

    /// <summary>
    /// Fails the run before a single measurement is taken if either arm of any
    /// pair disagrees with its partner. Covers the plain, duplicate-bearing,
    /// empty and null inputs, both sides of each threshold, and the malformed
    /// input whose validity gate the trims reorder relative to the write.
    /// </summary>
    private void AssertEquivalence()
    {
        AssertSequenceEqual(NormalizeTags_Baseline(_tags), NormalizeTags_Optimised(_tags), "NormalizeTags/plain");
        AssertSequenceEqual(
            NormalizeTags_Baseline(_tagsWithDuplicates),
            NormalizeTags_Optimised(_tagsWithDuplicates),
            "NormalizeTags/duplicates");
        AssertSequenceEqual(NormalizeTags_Baseline(null), NormalizeTags_Optimised(null), "NormalizeTags/null");
        AssertSequenceEqual(
            NormalizeTags_Baseline(Array.Empty<string>()),
            NormalizeTags_Optimised(Array.Empty<string>()),
            "NormalizeTags/empty");

        // Both sides of the dedup threshold, including the exact boundary.
        foreach (var width in new[] { 1, TagLinearDedupThreshold, TagLinearDedupThreshold + 1, 40 })
        {
            var sample = new string[width];
            for (var i = 0; i < width; i++)
            {
                // Deliberately collides every third entry so the dedup path is
                // taken at every width, not only the duplicate-bearing lane.
                sample[i] = "tag-" + (i - (i % 3)).ToString("D2", CultureInfo.InvariantCulture);
            }

            AssertSequenceEqual(
                NormalizeTags_Baseline(sample),
                NormalizeTags_Optimised(sample),
                "NormalizeTags/width-" + width.ToString(CultureInfo.InvariantCulture));
        }

        // A tag carrying the NUL separator must be rejected by both arms, and
        // rejected at the same element, because the trim moves where the result
        // is written relative to the validity gate.
        AssertBothReject(new[] { "ok", "bad\0tag" }, "NormalizeTags/separator");
        AssertBothReject(new[] { "ok", "" }, "NormalizeTags/empty-element");
        AssertBothReject(new[] { "ok", null! }, "NormalizeTags/null-element");

        // Row keys.
        AssertStringEqual(
            RowKey_Baseline("tag-01", TreeId, Key),
            RowKey_Optimised("tag-01", TreeId, Key),
            "RowKey");
        AssertStringEqual(
            KeyRowKey_Baseline(TreeId, Key, "tag-01"),
            KeyRowKey_Optimised(TreeId, Key, "tag-01"),
            "KeyRowKey");
        AssertStringEqual(
            KeyMajorPrefixFor_Baseline(TreeId, Key),
            KeyMajorPrefixFor_Optimised(TreeId, Key),
            "KeyMajorPrefixFor");

        // Empty segments are legal in the mirror prefix (a zero-length key is
        // rejected upstream, but the builder itself must not depend on that).
        AssertStringEqual(RowKey_Baseline("", "", ""), RowKey_Optimised("", "", ""), "RowKey/empty");
        AssertStringEqual(
            KeyMajorPrefixFor_Baseline("", ""),
            KeyMajorPrefixFor_Optimised("", ""),
            "KeyMajorPrefixFor/empty");

        // Reconcile, at both widths and on both partitions.
        foreach (var width in new[] { 1, LinearTagScanThreshold, LinearTagScanThreshold + 1, 40 })
        {
            var desired = new string[width];
            var current = new string[width];
            for (var i = 0; i < width; i++)
            {
                desired[i] = "tag-" + i.ToString("D2", CultureInfo.InvariantCulture);
                current[i] = "tag-" + (i + (width / 2)).ToString("D2", CultureInfo.InvariantCulture);
            }

            ReconcileBaseline(desired, current, out var baseAdd, out var baseRemove);
            ReconcileOptimised(desired, current, out var optAdd, out var optRemove);
            AssertSequenceEqual(
                baseAdd?.ToArray() ?? Array.Empty<string>(),
                optAdd?.ToArray() ?? Array.Empty<string>(),
                "Reconcile/add-" + width.ToString(CultureInfo.InvariantCulture));
            AssertSequenceEqual(
                baseRemove?.ToArray() ?? Array.Empty<string>(),
                optRemove?.ToArray() ?? Array.Empty<string>(),
                "Reconcile/remove-" + width.ToString(CultureInfo.InvariantCulture));
        }

        // The converged case - identical sets - must leave both partitions null
        // in both arms, which is the property the lazy list construction exists
        // to preserve.
        ReconcileBaseline(_tags, _tags, out var convergedAdd, out var convergedRemove);
        ReconcileOptimised(_tags, _tags, out var convergedOptAdd, out var convergedOptRemove);
        if (convergedAdd is not null || convergedRemove is not null
            || convergedOptAdd is not null || convergedOptRemove is not null)
        {
            throw new InvalidOperationException("Reconcile/converged: expected both partitions to stay null.");
        }
    }

    private static void AssertSequenceEqual(string[] expected, string[] actual, string what)
    {
        if (expected.Length != actual.Length)
        {
            throw new InvalidOperationException(
                $"{what}: length {actual.Length} != baseline {expected.Length}.");
        }

        for (var i = 0; i < expected.Length; i++)
        {
            if (!string.Equals(expected[i], actual[i], StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"{what}: element {i} '{actual[i]}' != baseline '{expected[i]}'.");
            }
        }
    }

    private static void AssertStringEqual(string expected, string actual, string what)
    {
        if (!string.Equals(expected, actual, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"{what}: '{actual}' != baseline '{expected}'.");
        }
    }

    private static void AssertBothReject(string[] tags, string what)
    {
        string? baselineType = null;
        string? optimisedType = null;
        try
        {
            NormalizeTags_Baseline(tags);
        }
        catch (ArgumentException ex)
        {
            baselineType = ex.GetType().Name;
        }

        try
        {
            NormalizeTags_Optimised(tags);
        }
        catch (ArgumentException ex)
        {
            optimisedType = ex.GetType().Name;
        }

        if (baselineType is null || optimisedType is null || baselineType != optimisedType)
        {
            throw new InvalidOperationException(
                $"{what}: baseline threw '{baselineType ?? "nothing"}', optimised threw '{optimisedType ?? "nothing"}'.");
        }
    }

    // ------------------------------------------------------------------
    // (1) NormalizeTags
    // ------------------------------------------------------------------

    [Benchmark(Baseline = true, Description = "NormalizeTags: List then ToArray (baseline)")]
    public string[] NormalizeTagsBaseline() => NormalizeTags_Baseline(_tags);

    [Benchmark(Description = "NormalizeTags: fill an exactly-sized array (optimised)")]
    public string[] NormalizeTagsOptimised() => NormalizeTags_Optimised(_tags);

    [Benchmark(Description = "NormalizeTags, duplicates: List then ToArray (baseline)")]
    public string[] NormalizeTagsDuplicatesBaseline() => NormalizeTags_Baseline(_tagsWithDuplicates);

    [Benchmark(Description = "NormalizeTags, duplicates: fill then trim (optimised)")]
    public string[] NormalizeTagsDuplicatesOptimised() => NormalizeTags_Optimised(_tagsWithDuplicates);

    /// <summary>Verbatim pre-trim <c>LatticeTagIndexContext.NormalizeTags</c>.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string[] NormalizeTags_Baseline(string[]? tags)
    {
        if (tags is null || tags.Length == 0)
        {
            return Array.Empty<string>();
        }

        var list = new List<string>(tags.Length);
        if (tags.Length <= TagLinearDedupThreshold)
        {
            foreach (var tag in tags)
            {
                ValidateTag(tag);
                if (!ContainsOrdinal(list, tag))
                {
                    list.Add(tag);
                }
            }
        }
        else
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                ValidateTag(tag);
                if (seen.Add(tag))
                {
                    list.Add(tag);
                }
            }
        }

        return list.ToArray();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string[] NormalizeTags_Optimised(string[]? tags)
    {
        if (tags is null || tags.Length == 0)
        {
            return Array.Empty<string>();
        }

        var result = new string[tags.Length];
        var count = 0;
        if (tags.Length <= TagLinearDedupThreshold)
        {
            foreach (var tag in tags)
            {
                ValidateTag(tag);
                if (!ContainsOrdinal(result, count, tag))
                {
                    result[count++] = tag;
                }
            }
        }
        else
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                ValidateTag(tag);
                if (seen.Add(tag))
                {
                    result[count++] = tag;
                }
            }
        }

        return count == tags.Length ? result : result[..count];
    }

    // ------------------------------------------------------------------
    // (2) Row-key construction
    // ------------------------------------------------------------------

    [Benchmark(Description = "Row keys: params-array string.Concat (baseline)")]
    public int RowKeysBaseline()
    {
        var total = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            total += RowKey_Baseline(_tags[i], TreeId, Key).Length;
            total += KeyRowKey_Baseline(TreeId, Key, _tags[i]).Length;
        }

        return total + KeyMajorPrefixFor_Baseline(TreeId, Key).Length;
    }

    [Benchmark(Description = "Row keys: string.Create (optimised)")]
    public int RowKeysOptimised()
    {
        var total = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            total += RowKey_Optimised(_tags[i], TreeId, Key).Length;
            total += KeyRowKey_Optimised(TreeId, Key, _tags[i]).Length;
        }

        return total + KeyMajorPrefixFor_Optimised(TreeId, Key).Length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string RowKey_Baseline(string tag, string treeId, string key) =>
        string.Concat(tag, SepStr, treeId, SepStr, key);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string KeyRowKey_Baseline(string treeId, string key, string tag) =>
        string.Concat(KeyMajorPrefix, treeId, SepStr, key, SepStr, tag);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string KeyMajorPrefixFor_Baseline(string treeId, string key) =>
        string.Concat(KeyMajorPrefix, treeId, SepStr, key, SepStr);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string RowKey_Optimised(string tag, string treeId, string key) =>
        string.Create(
            tag.Length + 1 + treeId.Length + 1 + key.Length,
            (tag, treeId, key),
            static (span, state) =>
            {
                var (tag, treeId, key) = state;
                tag.CopyTo(span);
                var at = tag.Length;
                span[at++] = Sep;
                treeId.CopyTo(span[at..]);
                at += treeId.Length;
                span[at++] = Sep;
                key.CopyTo(span[at..]);
            });

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string KeyRowKey_Optimised(string treeId, string key, string tag) =>
        string.Create(
            KeyMajorPrefix.Length + treeId.Length + 1 + key.Length + 1 + tag.Length,
            (treeId, key, tag),
            static (span, state) =>
            {
                var (treeId, key, tag) = state;
                KeyMajorPrefix.CopyTo(span);
                var at = KeyMajorPrefix.Length;
                treeId.CopyTo(span[at..]);
                at += treeId.Length;
                span[at++] = Sep;
                key.CopyTo(span[at..]);
                at += key.Length;
                span[at++] = Sep;
                tag.CopyTo(span[at..]);
            });

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string KeyMajorPrefixFor_Optimised(string treeId, string key) =>
        string.Create(
            KeyMajorPrefix.Length + treeId.Length + 1 + key.Length + 1,
            (treeId, key),
            static (span, state) =>
            {
                var (treeId, key) = state;
                KeyMajorPrefix.CopyTo(span);
                var at = KeyMajorPrefix.Length;
                treeId.CopyTo(span[at..]);
                at += treeId.Length;
                span[at++] = Sep;
                key.CopyTo(span[at..]);
                at += key.Length;
                span[at] = Sep;
            });

    // ------------------------------------------------------------------
    // (3) ReconcileTagSet's desired side
    // ------------------------------------------------------------------

    [Benchmark(Description = "Reconcile: HashSet desired set (baseline)")]
    public int ReconcileBaselineLane()
    {
        ReconcileBaseline(_tags, _current, out var toAdd, out var toRemove);
        return (toAdd?.Count ?? 0) + (toRemove?.Count ?? 0);
    }

    [Benchmark(Description = "Reconcile: branch to a linear desired list (optimised)")]
    public int ReconcileOptimisedLane()
    {
        ReconcileOptimised(_tags, _current, out var toAdd, out var toRemove);
        return (toAdd?.Count ?? 0) + (toRemove?.Count ?? 0);
    }

    /// <summary>
    /// Verbatim pre-trim shape: the caller validates and dedups into a
    /// <see cref="HashSet{T}"/> presized from the tag list, then reconciles
    /// against it.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ReconcileBaseline(
        IReadOnlyList<string> tags,
        IReadOnlyList<string> current,
        out List<string>? toAdd,
        out List<string>? toRemove)
    {
        var desired = new HashSet<string>(tags.Count, StringComparer.Ordinal);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            desired.Add(tag);
        }

        toAdd = null;
        toRemove = null;

        HashSet<string>? currentSet = null;
        if (current.Count > LinearTagScanThreshold)
        {
            currentSet = new HashSet<string>(current, StringComparer.Ordinal);
        }

        foreach (var tag in desired)
        {
            var present = currentSet is not null
                ? currentSet.Contains(tag)
                : ContainsOrdinal(current, tag);
            if (!present)
            {
                (toAdd ??= []).Add(tag);
            }
        }

        for (var i = 0; i < current.Count; i++)
        {
            var tag = current[i];
            if (!desired.Contains(tag))
            {
                (toRemove ??= []).Add(tag);
            }
        }
    }

    /// <summary>
    /// Trimmed shape: below <see cref="LinearTagScanThreshold"/> the desired
    /// side is deduped into a presized <see cref="List{T}"/> and answered by a
    /// linear ordinal scan, mirroring what the method already does for the
    /// <c>current</c> side. At and above the threshold it dispatches to the
    /// shipped hashing body, which lives in a method of its own so that the JIT
    /// compiles it exactly as it did before the trim.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ReconcileOptimised(
        IReadOnlyList<string> tags,
        IReadOnlyList<string> current,
        out List<string>? toAdd,
        out List<string>? toRemove)
    {
        if (tags.Count > LinearTagScanThreshold)
        {
            ReconcileHashedDesired(tags, current, out toAdd, out toRemove);
            return;
        }

        ReconcileLinearDesired(tags, current, out toAdd, out toRemove);
    }

    private static void ReconcileHashedDesired(
        IReadOnlyList<string> tags,
        IReadOnlyList<string> current,
        out List<string>? toAdd,
        out List<string>? toRemove)
    {
        toAdd = null;
        toRemove = null;

        var desired = new HashSet<string>(tags.Count, StringComparer.Ordinal);
        foreach (var candidate in tags)
        {
            ValidateTag(candidate);
            desired.Add(candidate);
        }

        HashSet<string>? currentSet = null;
        if (current.Count > LinearTagScanThreshold)
        {
            currentSet = new HashSet<string>(current, StringComparer.Ordinal);
        }

        foreach (var tag in desired)
        {
            var present = currentSet is not null
                ? currentSet.Contains(tag)
                : ContainsOrdinal(current, tag);
            if (!present)
            {
                (toAdd ??= []).Add(tag);
            }
        }

        for (var i = 0; i < current.Count; i++)
        {
            var tag = current[i];
            if (!desired.Contains(tag))
            {
                (toRemove ??= []).Add(tag);
            }
        }
    }

    private static void ReconcileLinearDesired(
        IReadOnlyList<string> tags,
        IReadOnlyList<string> current,
        out List<string>? toAdd,
        out List<string>? toRemove)
    {
        toAdd = null;
        toRemove = null;

        var desired = new List<string>(tags.Count);
        foreach (var candidate in tags)
        {
            ValidateTag(candidate);
            if (!ContainsOrdinalConcrete(desired, candidate))
            {
                desired.Add(candidate);
            }
        }

        HashSet<string>? currentSet = null;
        if (current.Count > LinearTagScanThreshold)
        {
            currentSet = new HashSet<string>(current, StringComparer.Ordinal);
        }

        for (var i = 0; i < desired.Count; i++)
        {
            var tag = desired[i];
            var present = currentSet is not null
                ? currentSet.Contains(tag)
                : ContainsOrdinal(current, tag);
            if (!present)
            {
                (toAdd ??= []).Add(tag);
            }
        }

        for (var i = 0; i < current.Count; i++)
        {
            var tag = current[i];
            if (!ContainsOrdinalConcrete(desired, tag))
            {
                (toRemove ??= []).Add(tag);
            }
        }
    }

    // ------------------------------------------------------------------
    // Shared helpers, mirrored from production
    // ------------------------------------------------------------------

    private static bool ContainsOrdinalConcrete(List<string> values, string value)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ContainsOrdinal(IReadOnlyList<string> values, string value)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ContainsOrdinal(string[] values, int count, string value)
    {
        for (var i = 0; i < count; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static void ValidateTag(string tag)
    {
        ArgumentException.ThrowIfNullOrEmpty(tag);
        if (tag.Contains(Sep))
        {
            throw new ArgumentException("A tag must not contain the NUL ('\\0') separator character.", nameof(tag));
        }
    }
}
