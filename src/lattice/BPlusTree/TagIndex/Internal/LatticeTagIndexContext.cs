using System.Runtime.CompilerServices;

namespace Orleans.Lattice;

/// <summary>
/// Internal orchestrator for a tag index. Holds the subject tree, the sibling
/// index tree (<c>tag-{indexName}</c>), and the grain factory, and composes all
/// tag-index operations from the public <see cref="ILattice"/> surface
/// (SetAsync / DeleteAsync / ExistsAsync for membership rows; the resilient
/// range scan for posting lists and reconcile). Membership rows are keyed
/// <c>tag \0 treeId \0 key</c>.
/// </summary>
internal sealed class LatticeTagIndexContext : ILatticeTagIndex
{
    private const char Sep = '\0';

    // String form of the separator, reused to avoid the per-call single-char
    // string allocation that `Sep.ToString()` would incur in the row/prefix
    // builders on the hot membership and query paths.
    private const string SepStr = "\0";

    // Reserved prefix for covered-tree marker rows (one idempotent row per
    // covered tree: `\0covered\0{treeId}`). Starts with NUL so markers sort
    // before, and never collide with, any membership row whose first segment is
    // a non-empty tag that cannot contain NUL.
    private const string CoveredMarkerPrefix = "\0covered\0";

    // Reserved prefix for key-major mirror rows (`\0k\0{treeId}\0{key}\0{tag}`).
    // Also NUL-led, so it shares the membership rows' reserved-row exclusion.
    private const string KeyMajorPrefix = "\0k\0";

    // Single shared flag value for every membership row. Never mutated, so it is
    // safe to reuse across all writes (avoids a per-row allocation).
    private static readonly byte[] Flag = [1];

    private readonly IGrainFactory _grainFactory;
    private readonly string _indexName;
    private readonly string _indexTreeId;
    private readonly ILattice _indexTree;
    private readonly string _subjectTreeId;
    private readonly ILattice _subjectTree;
    private readonly IReadOnlyCollection<string>? _allowlist;

    // Per-context caches. The accepted-tree set memoises open-mode existence
    // checks; the hint cache memoises the covered-tree set.
    private readonly HashSet<string> _acceptedTrees = new(StringComparer.Ordinal);
    private HashSet<string>? _hintCache;

    private LatticeTagIndexContext(
        IGrainFactory grainFactory,
        string indexName,
        string indexTreeId,
        ILattice indexTree,
        string subjectTreeId,
        ILattice subjectTree,
        IReadOnlyCollection<string>? allowlist)
    {
        _grainFactory = grainFactory;
        _indexName = indexName;
        _indexTreeId = indexTreeId;
        _indexTree = indexTree;
        _subjectTreeId = subjectTreeId;
        _subjectTree = subjectTree;
        _allowlist = allowlist;
    }

    internal static LatticeTagIndexContext Create(
        ILattice tree,
        IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowlist)
    {
        var indexTreeId = "tag-" + indexName;
        var indexTree = grainFactory.GetGrain<ILattice>(indexTreeId);
        var subjectTreeId = tree.GetPrimaryKeyString();
        return new LatticeTagIndexContext(grainFactory, indexName, indexTreeId, indexTree, subjectTreeId, tree, allowlist);
    }

    internal static ILatticeMultiTreeTagIndex CreateMultiTree(
        IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowlist)
    {
        var indexTreeId = "tag-" + indexName;
        var indexTree = grainFactory.GetGrain<ILattice>(indexTreeId);
        // No subject is pre-bound; the index tree id stands in as a harmless
        // placeholder because the multi-tree surface never issues subject-only
        // operations against it (it ranges over discovered covered trees).
        var ctx = new LatticeTagIndexContext(grainFactory, indexName, indexTreeId, indexTree, indexTreeId, indexTree, allowlist);
        return new MultiTreeView(ctx);
    }

    private LatticeTagIndexContext ForSubject(string treeId)
    {
        var subject = string.Equals(treeId, _subjectTreeId, StringComparison.Ordinal)
            ? _subjectTree
            : _grainFactory.GetGrain<ILattice>(treeId);
        return new LatticeTagIndexContext(_grainFactory, _indexName, _indexTreeId, _indexTree, treeId, subject, _allowlist)
        {
            _hintCache = _hintCache,
        };
    }

    // ── ILatticeTagIndex ─────────────────────────────────────────────

    public string IndexName => _indexName;

    public string TreeId => _subjectTreeId;

    public ILatticeKeyTags Key(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new KeyTags(this, _subjectTreeId, key);
    }

    public ILatticeTagQuery WithAllTags(params string[] tags) =>
        new TagQuery(this, _subjectTreeId, NormalizeTags(tags), all: true);

    public ILatticeTagQuery WithAnyTags(params string[] tags) =>
        new TagQuery(this, _subjectTreeId, NormalizeTags(tags), all: false);

    public ILatticeValueTagWrite SetValueWithTags(string key, byte[] value, params string[] tags)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        return new ValueTagWrite(this, _subjectTreeId, key, value, NormalizeTags(tags));
    }

    public Task<TagReconcileReport> ReconcileAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
        ReconcileSubjectAsync(_subjectTreeId, startInclusive, endExclusive, cancellationToken);

    public IAsyncEnumerable<string> TagsAsync(CancellationToken cancellationToken = default) =>
        EnumerateTagsAsync(_subjectTreeId, cancellationToken);

    public ILatticeMultiTreeTagIndex MultiTree() => new MultiTreeView(this);

    // ── Membership row helpers ───────────────────────────────────────

    private static string RowKey(string tag, string treeId, string key) =>
        string.Concat(tag, SepStr, treeId, SepStr, key);

    private static string PostingPrefix(string tag, string treeId) =>
        string.Concat(tag, SepStr, treeId, SepStr);

    // Key-major mirror of a membership row: `\0k \0 treeId \0 key \0 tag`. Written
    // and deleted alongside the tag-major row so the inverse direction
    // (key -> tags) is a bounded prefix scan rather than a full index scan. The
    // `\0` prefix keeps mirrors out of the membership namespace, so every
    // `rowKey[0] == Sep` guard and every tag/posting/marker prefix skips them.
    private static string KeyRowKey(string treeId, string key, string tag) =>
        string.Concat(KeyMajorPrefix, treeId, SepStr, key, SepStr, tag);

    private static string KeyMajorPrefixFor(string treeId, string key) =>
        string.Concat(KeyMajorPrefix, treeId, SepStr, key, SepStr);

    private static string PrefixEnd(string prefix)
    {
        // prefix ends with Sep ('\0'); the smallest string greater than every
        // string with this prefix increments that trailing separator.
        var chars = prefix.ToCharArray();
        chars[^1] = (char)(chars[^1] + 1);
        return new string(chars);
    }

    private static bool TryParseRow(string rowKey, out string tag, out string treeId, out string key)
    {
        tag = treeId = key = string.Empty;
        var first = rowKey.IndexOf(Sep);
        if (first < 0) return false;
        var second = rowKey.IndexOf(Sep, first + 1);
        if (second < 0) return false;
        tag = rowKey[..first];
        treeId = rowKey.Substring(first + 1, second - first - 1);
        key = rowKey[(second + 1)..];
        return tag.Length > 0;
    }

    internal async IAsyncEnumerable<string> PostingListAsync(string treeId, string tag, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var prefix = PostingPrefix(tag, treeId);
        var end = PrefixEnd(prefix);
        await foreach (var rowKey in _indexTree.ScanKeysAsync(prefix, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return rowKey[prefix.Length..];
        }
    }

    internal async IAsyncEnumerable<string> QueryAsync(string treeId, string[] tags, bool all, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (tags.Length == 0)
        {
            yield break;
        }

        if (tags.Length == 1)
        {
            await foreach (var key in PostingListAsync(treeId, tags[0], cancellationToken).ConfigureAwait(false))
            {
                yield return key;
            }
            yield break;
        }

        if (all)
        {
            // Stream the first tag's posting list; emit a key only when every
            // other tag's membership row also exists for it.
            await foreach (var key in PostingListAsync(treeId, tags[0], cancellationToken).ConfigureAwait(false))
            {
                var inAll = true;
                for (var i = 1; i < tags.Length && inAll; i++)
                {
                    inAll = await _indexTree.ExistsAsync(RowKey(tags[i], treeId, key), cancellationToken).ConfigureAwait(false);
                }
                if (inAll)
                {
                    yield return key;
                }
            }
        }
        else
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                await foreach (var key in PostingListAsync(treeId, tag, cancellationToken).ConfigureAwait(false))
                {
                    if (seen.Add(key))
                    {
                        yield return key;
                    }
                }
            }
        }
    }

    internal async Task<IReadOnlyList<string>> GetTagsForKeyAsync(string treeId, string key, CancellationToken cancellationToken)
    {
        // Bounded prefix scan over the key-major mirror rows. The range may
        // over-match when keys share a `key\0...` prefix (only possible when a
        // key itself contains NUL), so each row's full key is compared exactly.
        var prefix = KeyMajorPrefixFor(treeId, key);
        var headLen = KeyMajorPrefix.Length + treeId.Length + 1; // up to and incl. the treeId separator
        var end = PrefixEnd(prefix);
        var result = new List<string>();
        await foreach (var rowKey in _indexTree.ScanKeysAsync(prefix, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // rowKey == `\0k\0{treeId}\0{fullKey}\0{tag}`; tag is the final
            // segment, fullKey is everything between the treeId separator and it.
            var body = rowKey[headLen..];
            var lastSep = body.LastIndexOf(Sep);
            if (lastSep < 0)
            {
                continue;
            }
            var fullKey = body[..lastSep];
            if (!string.Equals(fullKey, key, StringComparison.Ordinal))
            {
                continue;
            }
            var tag = body[(lastSep + 1)..];
            if (tag.Length > 0)
            {
                result.Add(tag);
            }
        }
        result.Sort(StringComparer.Ordinal);
        return result;
    }

    // Streams the distinct tags found across membership rows, in ascending
    // ordinal order. When <paramref name="onlyTree"/> is non-null only tags with
    // at least one member key in that tree are emitted; when null every tag in
    // the index is emitted (multi-tree surface). Rows for a given tag are
    // contiguous because the scan is ordered and the tag is the leading segment,
    // so a single tag of state suffices to dedupe and stream without buffering.
    internal async IAsyncEnumerable<string> EnumerateTagsAsync(string? onlyTree, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        string? currentTag = null;
        var emitted = false;
        await foreach (var rowKey in _indexTree.ScanKeysAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (rowKey.Length > 0 && rowKey[0] == Sep)
            {
                continue; // reserved hint row
            }
            if (!TryParseRow(rowKey, out var tag, out var treeId, out _))
            {
                continue;
            }
            if (!string.Equals(tag, currentTag, StringComparison.Ordinal))
            {
                currentTag = tag;
                emitted = false;
            }
            if (emitted)
            {
                continue;
            }
            if (onlyTree is null || string.Equals(treeId, onlyTree, StringComparison.Ordinal))
            {
                emitted = true;
                yield return tag;
            }
        }
    }

    internal async Task AddTagsForKeyAsync(string treeId, string key, IReadOnlyList<string> tags, CancellationToken cancellationToken)
    {
        if (tags.Count == 0)
        {
            return;
        }
        CheckAllowlist(treeId);
        await EnsureExistsAsync(treeId, cancellationToken).ConfigureAwait(false);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            await _indexTree.SetAsync(RowKey(tag, treeId, key), Flag, cancellationToken).ConfigureAwait(false);
            await _indexTree.SetAsync(KeyRowKey(treeId, key, tag), Flag, cancellationToken).ConfigureAwait(false);
        }
        await EnsureHintAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    internal async Task RemoveTagsForKeyAsync(string treeId, string key, IReadOnlyList<string> tags, CancellationToken cancellationToken)
    {
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            await _indexTree.DeleteAsync(RowKey(tag, treeId, key), cancellationToken).ConfigureAwait(false);
            await _indexTree.DeleteAsync(KeyRowKey(treeId, key, tag), cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task SetTagsForKeyAsync(string treeId, string key, IReadOnlyList<string> tags, CancellationToken cancellationToken)
    {
        var desired = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            desired.Add(tag);
        }
        var current = await GetTagsForKeyAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        var currentSet = new HashSet<string>(current, StringComparer.Ordinal);

        var toAdd = new List<string>();
        foreach (var tag in desired)
        {
            if (!currentSet.Contains(tag))
            {
                toAdd.Add(tag);
            }
        }
        var toRemove = new List<string>();
        foreach (var tag in currentSet)
        {
            if (!desired.Contains(tag))
            {
                toRemove.Add(tag);
            }
        }

        if (toAdd.Count > 0)
        {
            await AddTagsForKeyAsync(treeId, key, toAdd, cancellationToken).ConfigureAwait(false);
        }
        if (toRemove.Count > 0)
        {
            await RemoveTagsForKeyAsync(treeId, key, toRemove, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task CommitValueWithTagsAsync(string treeId, string key, byte[] value, string[] tags, TagConsistency consistency, CancellationToken cancellationToken)
    {
        var subject = SubjectTreeFor(treeId);

        if (consistency == TagConsistency.Atomic && tags.Length > 0)
        {
            // The value write inside the saga registers the subject tree, so a
            // closed allowlist is still enforced but open-mode existence is not
            // pre-checked (the saga creates the tree atomically).
            CheckAllowlist(treeId);
            var opId = "tagval-" + Guid.NewGuid().ToString("N");
            var builder = _grainFactory.BeginAtomicWrite(opId)
                .ForTree(treeId).Set(key, value)
                .ForTree(_indexTreeId);
            foreach (var tag in tags)
            {
                ValidateTag(tag);
                builder.Set(RowKey(tag, treeId, key), Flag);
                builder.Set(KeyRowKey(treeId, key, tag), Flag);
            }
            await builder.CommitAsync(cancellationToken).ConfigureAwait(false);
            await EnsureHintAsync(treeId, cancellationToken).ConfigureAwait(false);
            return;
        }

        // Eventual (or atomic with no tags): two independent durable writes.
        await subject.SetAsync(key, value, cancellationToken).ConfigureAwait(false);
        if (tags.Length > 0)
        {
            await AddTagsForKeyAsync(treeId, key, tags, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<TagReconcileReport> ReconcileSubjectAsync(string treeId, string? startInclusive, string? endExclusive, CancellationToken cancellationToken)
    {
        var subject = SubjectTreeFor(treeId);

        var live = new HashSet<string>(StringComparer.Ordinal);
        var keysScanned = 0;
        await foreach (var k in subject.ScanKeysAsync(startInclusive, endExclusive, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            live.Add(k);
            keysScanned++;
        }

        var rowsScanned = 0;
        var orphans = 0;
        await foreach (var rowKey in _indexTree.ScanKeysAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (rowKey.Length > 0 && rowKey[0] == Sep)
            {
                continue; // reserved hint row
            }
            if (!TryParseRow(rowKey, out var rowTag, out var rowTree, out var key))
            {
                continue;
            }
            if (!string.Equals(rowTree, treeId, StringComparison.Ordinal))
            {
                continue;
            }
            if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
            {
                continue;
            }
            if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
            {
                continue;
            }
            rowsScanned++;
            if (!live.Contains(key))
            {
                // Re-verify against the subject before deleting: the live set is
                // a point-in-time snapshot, so a key written concurrently after
                // the snapshot (value first, then tags) could otherwise have its
                // freshly-committed tag rows destroyed as false orphans.
                if (await subject.ExistsAsync(key, cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }
                await _indexTree.DeleteAsync(rowKey, cancellationToken).ConfigureAwait(false);
                await _indexTree.DeleteAsync(KeyRowKey(rowTree, key, rowTag), cancellationToken).ConfigureAwait(false);
                orphans++;
            }
        }

        return new TagReconcileReport(1, keysScanned, rowsScanned, orphans);
    }

    internal async Task<TagReconcileReport> ReconcileAllAsync(string? startInclusive, string? endExclusive, CancellationToken cancellationToken)
    {
        var trees = await GetCoveredTreesAsync(cancellationToken).ConfigureAwait(false);
        var report = TagReconcileReport.Empty;
        foreach (var treeId in trees)
        {
            report = report.Combine(await ReconcileSubjectAsync(treeId, startInclusive, endExclusive, cancellationToken).ConfigureAwait(false));
        }
        return report;
    }

    internal async IAsyncEnumerable<TaggedKey> MultiQueryAsync(string[] tags, bool all, string? onlyTree, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IReadOnlyList<string> trees = onlyTree is not null
            ? new[] { onlyTree }
            : await GetCoveredTreesAsync(cancellationToken).ConfigureAwait(false);

        foreach (var treeId in trees)
        {
            await foreach (var key in QueryAsync(treeId, tags, all, cancellationToken).ConfigureAwait(false))
            {
                yield return new TaggedKey(treeId, key);
            }
        }
    }

    // ── Covered-tree markers ─────────────────────────────────────────

    internal async Task<IReadOnlyList<string>> GetCoveredTreesAsync(CancellationToken cancellationToken)
    {
        // Marker rows are authoritative and read fresh on every call: each is an
        // independent idempotent row, so concurrent first-time writes from
        // different subjects never clobber each other (unlike a single joined
        // blob), and a long-lived index object can never serve a stale set.
        var set = new SortedSet<string>(StringComparer.Ordinal);
        var end = PrefixEnd(CoveredMarkerPrefix);
        await foreach (var rowKey in _indexTree.ScanKeysAsync(CoveredMarkerPrefix, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            set.Add(rowKey[CoveredMarkerPrefix.Length..]);
        }

        if (set.Count > 0)
        {
            _hintCache = new HashSet<string>(set, StringComparer.Ordinal);
            return set.ToList();
        }

        // Self-heal: no markers yet (e.g. an index populated before markers
        // existed). Rebuild the distinct covered-tree set from a full membership
        // self-scan and persist a marker for each discovered tree.
        await foreach (var rowKey in _indexTree.ScanKeysAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (rowKey.Length > 0 && rowKey[0] == Sep)
            {
                continue;
            }
            if (TryParseRow(rowKey, out _, out var treeId, out _))
            {
                set.Add(treeId);
            }
        }
        foreach (var treeId in set)
        {
            await _indexTree.SetAsync(CoveredMarkerKey(treeId), Flag, cancellationToken).ConfigureAwait(false);
        }
        _hintCache = new HashSet<string>(set, StringComparer.Ordinal);
        return set.ToList();
    }

    private async Task EnsureHintAsync(string treeId, CancellationToken cancellationToken)
    {
        // The cache is only a write-dedup memo here (reads go to GetCoveredTrees-
        // Async, which is authoritative): a stale cache at worst issues a
        // redundant idempotent marker write, never a missed one.
        if (_hintCache is not null && _hintCache.Contains(treeId))
        {
            return;
        }
        await _indexTree.SetAsync(CoveredMarkerKey(treeId), Flag, cancellationToken).ConfigureAwait(false);
        (_hintCache ??= new HashSet<string>(StringComparer.Ordinal)).Add(treeId);
    }

    private static string CoveredMarkerKey(string treeId) => string.Concat(CoveredMarkerPrefix, treeId);

    // ── Acceptance validation ────────────────────────────────────────

    private ILattice SubjectTreeFor(string treeId) =>
        string.Equals(treeId, _subjectTreeId, StringComparison.Ordinal)
            ? _subjectTree
            : _grainFactory.GetGrain<ILattice>(treeId);

    private void CheckAllowlist(string treeId)
    {
        if (_allowlist is not null && !_allowlist.Contains(treeId))
        {
            throw new ArgumentException($"Tree '{treeId}' is not in the tag index's allowed-tree set.", nameof(treeId));
        }
    }

    private async Task EnsureExistsAsync(string treeId, CancellationToken cancellationToken)
    {
        if (_allowlist is not null)
        {
            return; // closed mode is enforced by CheckAllowlist
        }
        if (_acceptedTrees.Contains(treeId))
        {
            return;
        }
        var tree = SubjectTreeFor(treeId);
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"Tree '{treeId}' is not registered; write a value to it (e.g. via SetValueWithTags) before tagging its keys, or supply a closed allowlist of accepted trees.");
        }
        _acceptedTrees.Add(treeId);
    }

    private static string[] NormalizeTags(string[]? tags)
    {
        if (tags is null || tags.Length == 0)
        {
            return Array.Empty<string>();
        }
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var list = new List<string>(tags.Length);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            if (seen.Add(tag))
            {
                list.Add(tag);
            }
        }
        return list.ToArray();
    }

    private static void ValidateTag(string tag)
    {
        ArgumentException.ThrowIfNullOrEmpty(tag);
        if (tag.Contains(Sep))
        {
            throw new ArgumentException("A tag must not contain the NUL ('\\0') separator character.", nameof(tag));
        }
    }

    private static void ValidateTreeId(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (treeId.Contains(Sep))
        {
            throw new ArgumentException("A tree id must not contain the NUL ('\\0') separator character.", nameof(treeId));
        }
    }

    // ── Facade implementations ───────────────────────────────────────

    private sealed class KeyTags(LatticeTagIndexContext ctx, string treeId, string key) : ILatticeKeyTags
    {
        public Task<IReadOnlyList<string>> GetAsync(CancellationToken cancellationToken = default) =>
            ctx.GetTagsForKeyAsync(treeId, key, cancellationToken);

        public Task SetAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(tags);
            return ctx.SetTagsForKeyAsync(treeId, key, Materialize(tags), cancellationToken);
        }

        public Task AddAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(tags);
            return ctx.AddTagsForKeyAsync(treeId, key, Materialize(tags), cancellationToken);
        }

        public Task RemoveAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(tags);
            return ctx.RemoveTagsForKeyAsync(treeId, key, Materialize(tags), cancellationToken);
        }

        private static IReadOnlyList<string> Materialize(IEnumerable<string> tags) =>
            tags as IReadOnlyList<string> ?? tags.ToList();
    }

    private sealed class TagQuery(LatticeTagIndexContext ctx, string treeId, string[] tags, bool all) : ILatticeTagQuery
    {
        public IAsyncEnumerator<string> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            ctx.QueryAsync(treeId, tags, all, cancellationToken).GetAsyncEnumerator(cancellationToken);

        public async Task<int> CountAsync(CancellationToken cancellationToken = default)
        {
            var count = 0;
            await foreach (var _ in ctx.QueryAsync(treeId, tags, all, cancellationToken).ConfigureAwait(false))
            {
                count++;
            }
            return count;
        }
    }

    private sealed class ValueTagWrite(LatticeTagIndexContext ctx, string treeId, string key, byte[] value, string[] tags) : ILatticeValueTagWrite
    {
        private TagConsistency _consistency = TagConsistency.Eventual;

        public ILatticeValueTagWrite Atomic()
        {
            _consistency = TagConsistency.Atomic;
            return this;
        }

        public ILatticeValueTagWrite Eventual()
        {
            _consistency = TagConsistency.Eventual;
            return this;
        }

        public Task CommitAsync(CancellationToken cancellationToken = default) =>
            ctx.CommitValueWithTagsAsync(treeId, key, value, tags, _consistency, cancellationToken);
    }

    private sealed class MultiTreeView(LatticeTagIndexContext ctx) : ILatticeMultiTreeTagIndex
    {
        public string IndexName => ctx._indexName;

        public ILatticeTagIndex Tree(string treeId)
        {
            ValidateTreeId(treeId);
            return ctx.ForSubject(treeId);
        }

        public ILatticeMultiTreeTagQuery WithAllTags(params string[] tags) =>
            new MultiTagQuery(ctx, NormalizeTags(tags), all: true, onlyTree: null);

        public ILatticeMultiTreeTagQuery WithAnyTags(params string[] tags) =>
            new MultiTagQuery(ctx, NormalizeTags(tags), all: false, onlyTree: null);

        public Task<IReadOnlyList<string>> CoveredTreesAsync(CancellationToken cancellationToken = default) =>
            ctx.GetCoveredTreesAsync(cancellationToken);

        public IAsyncEnumerable<string> TagsAsync(CancellationToken cancellationToken = default) =>
            ctx.EnumerateTagsAsync(onlyTree: null, cancellationToken);

        public Task<TagReconcileReport> ReconcileAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
            ctx.ReconcileAllAsync(startInclusive, endExclusive, cancellationToken);
    }

    private sealed class MultiTagQuery(LatticeTagIndexContext ctx, string[] tags, bool all, string? onlyTree) : ILatticeMultiTreeTagQuery
    {
        public ILatticeMultiTreeTagQuery InTree(string treeId)
        {
            ValidateTreeId(treeId);
            return new MultiTagQuery(ctx, tags, all, treeId);
        }

        public IAsyncEnumerator<TaggedKey> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            ctx.MultiQueryAsync(tags, all, onlyTree, cancellationToken).GetAsyncEnumerator(cancellationToken);

        public async Task<int> CountAsync(CancellationToken cancellationToken = default)
        {
            var count = 0;
            await foreach (var _ in ctx.MultiQueryAsync(tags, all, onlyTree, cancellationToken).ConfigureAwait(false))
            {
                count++;
            }
            return count;
        }
    }
}
