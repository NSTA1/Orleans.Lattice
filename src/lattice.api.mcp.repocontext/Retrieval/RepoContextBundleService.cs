using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-only adapter behind the <c>repocontext_context</c> tool: a budgeted,
/// one-call context bundler. It runs a single search for a natural-language task,
/// resolves the ranked hits to unique source files, renders each at a chosen detail
/// level (path, outline, or bounded body), and packs as many as fit under a
/// <b>hard</b> token ceiling measured with the shared exact-BPE
/// <see cref="IRepoContextTokenCounter"/> - collapsing the search -> recall -> read
/// loop into one round trip that can never overrun the caller's context budget.
/// <para>
/// <b>Fail-closed and honest.</b> The ceiling is never exceeded: when even the
/// cheapest candidate does not fit, the bundle is emitted empty with a
/// guaranteed-to-fit <see cref="RepoContextContextResult.RetryBudgetTokens"/> rather
/// than a partial overrun. It reuses the existing seams - the search service (which
/// itself degrades to keyword ranking when no embedder is bound, so a bundle is
/// still produced), and the graph service's outline projection for outline detail -
/// and adds no storage primitive of its own.
/// </para>
/// </summary>
internal sealed class RepoContextBundleService
{
    private const int DefaultTop = 10;
    private const int MinTop = 1;
    private const int MaxTop = 50;
    private const int DefaultBudgetTokens = 8192;
    private const int MinBudgetTokens = 1;
    private const int MaxBudgetTokens = 200_000;

    private readonly RepoContextSearchService _search;
    private readonly RepoContextGraphService _graph;
    private readonly IGrainFactory _grainFactory;
    private readonly Orleans.Serialization.Serializer _serializer;
    private readonly IRepoContextTokenCounter _tokenCounter;

    /// <summary>Creates the bundle service.</summary>
    /// <param name="search">The search service used to rank source for the task. Must not be <see langword="null"/>.</param>
    /// <param name="graph">The graph service whose outline projection backs outline detail. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to hydrate file bodies and token counts. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode stored records. Must not be <see langword="null"/>.</param>
    /// <param name="tokenCounter">The shared exact-BPE token counter used to measure and budget the bundle. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextBundleService(
        RepoContextSearchService search,
        RepoContextGraphService graph,
        IGrainFactory grainFactory,
        Orleans.Serialization.Serializer serializer,
        IRepoContextTokenCounter tokenCounter)
    {
        ArgumentNullException.ThrowIfNull(search);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(tokenCounter);

        _search = search;
        _graph = graph;
        _grainFactory = grainFactory;
        _serializer = serializer;
        _tokenCounter = tokenCounter;
    }

    /// <summary>
    /// Builds a budgeted context bundle for <paramref name="task"/>. All three numeric
    /// and enum inputs are clamped defensively, so a wire caller can never drive
    /// unbounded work: <paramref name="top"/> to [1, 50], <paramref name="responseBudgetTokens"/>
    /// to [1, 200000], and an unrecognised <paramref name="detail"/> to
    /// <see cref="RepoContextContextDetail.Auto"/>.
    /// </summary>
    /// <param name="repoId">The repository to bundle from. Must not be <see langword="null"/>.</param>
    /// <param name="task">The natural-language task to pack context for. Must not be <see langword="null"/>.</param>
    /// <param name="top">The maximum number of files to consider; clamped to [1, 50], defaulting to 10 for a non-positive value.</param>
    /// <param name="responseBudgetTokens">The hard token ceiling; clamped to [1, 200000], defaulting to 8192 for a non-positive value.</param>
    /// <param name="detail">The requested detail level; an unrecognised value resolves to auto.</param>
    /// <param name="cancellationToken">Cancels the bundle.</param>
    /// <returns>The packed bundle, whose total never exceeds the clamped budget.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public async Task<RepoContextContextResult> BuildAsync(
        string repoId,
        string task,
        int top,
        int responseBudgetTokens,
        RepoContextContextDetail detail,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(task);

        var clampedTop = ClampTop(top);
        var budget = ClampBudget(responseBudgetTokens);
        var floorLevel = detail == RepoContextContextDetail.Auto ? RepoContextContextDetail.Paths : detail;

        var search = await _search.SearchAsync(repoId, task, clampedTop, cancellationToken).ConfigureAwait(false);
        var candidates = ResolveCandidates(search.Hits, clampedTop);

        if (candidates.Count == 0)
        {
            return new RepoContextContextResult
            {
                RepoId = repoId,
                Task = task,
                Mode = search.Mode,
                Detail = Label(floorLevel),
                BudgetTokens = budget,
                TotalTokens = 0,
                Truncated = false,
                RetryBudgetTokens = null,
                Entries = [],
            };
        }

        var levels = LevelsFor(detail);
        RepoContextBundlePacker.PackOutcome packed = default;
        var usedLevel = floorLevel;

        foreach (var level in levels)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var inputs = await RenderAsync(repoId, candidates, level, cancellationToken).ConfigureAwait(false);
            packed = RepoContextBundlePacker.Pack(inputs, budget, _tokenCounter);
            usedLevel = level;
            if (packed.Entries.Count > 0)
            {
                break;
            }
        }

        var fitted = packed.Entries.Count > 0;
        return new RepoContextContextResult
        {
            RepoId = repoId,
            Task = task,
            Mode = search.Mode,
            Detail = Label(fitted ? usedLevel : floorLevel),
            BudgetTokens = budget,
            TotalTokens = packed.TotalTokens,
            Truncated = packed.Truncated,
            // Fail closed: when nothing fit, every candidate was measured against an
            // empty bundle, so the cheapest candidate's cost strictly exceeds the
            // budget and is a guaranteed-to-fit retry budget.
            RetryBudgetTokens = fitted ? null : packed.MinCandidateTokens,
            Entries = packed.Entries,
        };
    }

    private static int ClampTop(int top)
        => top <= 0 ? DefaultTop : Math.Clamp(top, MinTop, MaxTop);

    private static int ClampBudget(int budget)
        => budget <= 0 ? DefaultBudgetTokens : Math.Clamp(budget, MinBudgetTokens, MaxBudgetTokens);

    private static string Label(RepoContextContextDetail level) => level switch
    {
        RepoContextContextDetail.Slices => "slices",
        RepoContextContextDetail.Outline => "outline",
        _ => "paths",
    };

    private static RepoContextContextDetail[] LevelsFor(RepoContextContextDetail detail) => detail switch
    {
        RepoContextContextDetail.Slices => [RepoContextContextDetail.Slices],
        RepoContextContextDetail.Outline => [RepoContextContextDetail.Outline],
        RepoContextContextDetail.Paths => [RepoContextContextDetail.Paths],
        _ => [RepoContextContextDetail.Slices, RepoContextContextDetail.Outline, RepoContextContextDetail.Paths],
    };

    /// <summary>
    /// Resolves the search hits to unique source files in rank order, keeping the
    /// best-ranked hit per path and capping the set at <paramref name="limit"/>. Hits
    /// that do not address a source file (memory, package, or repo roots) are skipped.
    /// </summary>
    private static List<PackCandidate> ResolveCandidates(IReadOnlyList<RepoContextSearchHit> hits, int limit)
    {
        var candidates = new List<PackCandidate>(Math.Min(hits.Count, limit));
        var seen = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < hits.Count && candidates.Count < limit; i++)
        {
            var hit = hits[i];
            var path = ResolvePath(hit.Entry);
            if (path is null || !seen.Add(path))
            {
                continue;
            }

            candidates.Add(new PackCandidate(path, hit.Score, hit.Reasons));
        }

        return candidates;
    }

    private static string? ResolvePath(RepoContextEntryView entry)
    {
        if (!string.IsNullOrEmpty(entry.Path))
        {
            return entry.Path;
        }

        return entry.Fields.TryGetValue("filePath", out var filePath) && !string.IsNullOrEmpty(filePath)
            ? filePath
            : null;
    }

    /// <summary>
    /// Renders every candidate to its packed content at <paramref name="level"/>,
    /// caching each per-file read so an auto degrade across levels never re-reads the
    /// same record. Returns the packer's candidate list in rank order.
    /// </summary>
    private async Task<IReadOnlyList<RepoContextBundlePacker.Candidate>> RenderAsync(
        string repoId,
        List<PackCandidate> candidates,
        RepoContextContextDetail level,
        CancellationToken cancellationToken)
    {
        var rendered = new List<RepoContextBundlePacker.Candidate>(candidates.Count);
        foreach (var candidate in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string content;
            int? fullRead;
            switch (level)
            {
                case RepoContextContextDetail.Slices:
                    var body = await candidate.GetBodyAsync(repoId, _grainFactory, _serializer, cancellationToken)
                        .ConfigureAwait(false);
                    fullRead = await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                        .ConfigureAwait(false);
                    if (string.IsNullOrEmpty(body))
                    {
                        content = candidate.Path;
                    }
                    else
                    {
                        content = body;
                        fullRead ??= _tokenCounter.CountTokens(body);
                    }

                    break;

                case RepoContextContextDetail.Outline:
                    var outline = await candidate.GetOutlineAsync(repoId, _graph, cancellationToken)
                        .ConfigureAwait(false);
                    content = outline.Length == 0 ? candidate.Path : outline;
                    fullRead = candidate.OutlineFullReadTokenCount;
                    break;

                default:
                    content = candidate.Path;
                    fullRead = await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                        .ConfigureAwait(false);
                    break;
            }

            rendered.Add(new RepoContextBundlePacker.Candidate(
                candidate.Path, candidate.Score, candidate.Reasons, content, fullRead));
        }

        return rendered;
    }

    /// <summary>
    /// A mutable per-file work item carried through the render passes. It caches each
    /// distinct store read (stored token count, body text, outline skeleton) so an
    /// auto degrade from slices to outline to paths reads each record at most once.
    /// </summary>
    private sealed class PackCandidate(string path, double score, IReadOnlyList<string> reasons)
    {
        private bool _storedTokenResolved;
        private int? _storedTokenCount;
        private bool _bodyResolved;
        private string? _body;
        private bool _outlineResolved;
        private string _outline = string.Empty;

        public string Path { get; } = path;

        public double Score { get; } = score;

        public IReadOnlyList<string> Reasons { get; } = reasons;

        public int? OutlineFullReadTokenCount { get; private set; }

        public async ValueTask<int?> GetStoredTokenCountAsync(
            string repoId,
            IGrainFactory grainFactory,
            Orleans.Serialization.Serializer serializer,
            CancellationToken cancellationToken)
        {
            if (_storedTokenResolved)
            {
                return _storedTokenCount;
            }

            var structural = grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
            var nodeBytes = await structural.GetAsync(RepoContextKeys.File(repoId, Path), cancellationToken)
                .ConfigureAwait(false);
            var count = nodeBytes is null
                ? null
                : RepoContextValues.ReadInt64(serializer.Deserialize<FileNode>(nodeBytes).TokenCount);

            _storedTokenCount = count is { } value ? (int)value : null;
            _storedTokenResolved = true;
            return _storedTokenCount;
        }

        public async ValueTask<string?> GetBodyAsync(
            string repoId,
            IGrainFactory grainFactory,
            Orleans.Serialization.Serializer serializer,
            CancellationToken cancellationToken)
        {
            if (_bodyResolved)
            {
                return _body;
            }

            var contentTree = grainFactory.GetGrain<ILattice>(RepoContextTrees.Content);
            var contentBytes = await contentTree.GetAsync(RepoContextKeys.Content(repoId, Path), cancellationToken)
                .ConfigureAwait(false);
            _body = contentBytes is null
                ? null
                : RepoContextValues.ReadString(serializer.Deserialize<ContentRecord>(contentBytes).Text);
            _bodyResolved = true;
            return _body;
        }

        public async ValueTask<string> GetOutlineAsync(
            string repoId,
            RepoContextGraphService graph,
            CancellationToken cancellationToken)
        {
            if (_outlineResolved)
            {
                return _outline;
            }

            var outline = await graph.OutlineAsync(repoId, Path, cancellationToken).ConfigureAwait(false);
            OutlineFullReadTokenCount = outline.FullReadTokenCount;
            _outline = RenderOutline(outline);
            _outlineResolved = true;
            return _outline;
        }

        private static string RenderOutline(RepoContextOutlineResult outline)
        {
            if (outline.Symbols.Count == 0)
            {
                return string.Empty;
            }

            var builder = new StringBuilder();
            for (var i = 0; i < outline.Symbols.Count; i++)
            {
                var symbol = outline.Symbols[i];
                if (i > 0)
                {
                    builder.Append('\n');
                }

                builder.Append(symbol.Signature.Length != 0 ? symbol.Signature : symbol.FullyQualifiedName);
            }

            return builder.ToString();
        }
    }
}
