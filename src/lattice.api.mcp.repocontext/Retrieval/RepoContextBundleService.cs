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
/// <b>Reuse economics.</b> A caller can hand back the opaque receipts of units it
/// already holds (<c>seen</c>), assert whole-file possession of a specific version
/// (<c>known</c>), and carry a named <c>session</c> so the tool remembers across calls
/// exactly what it already delivered. Content the caller already holds is suppressed
/// rather than re-delivered, is acknowledged in
/// <see cref="RepoContextContextResult.Reused"/>, and is <b>never</b> charged against
/// the token budget or the file count - so an agent never pays twice for the same
/// context. The load-bearing guard: a whole-file possession claim is honoured only for
/// a version this tool actually delivered as a complete body, so partial evidence (an
/// outline or a path) can never be promoted to whole-file possession.
/// </para>
/// <para>
/// <b>Fail-closed and honest.</b> The ceiling is never exceeded: when even the
/// cheapest candidate does not fit, the bundle is emitted empty with a
/// guaranteed-to-fit <see cref="RepoContextContextResult.RetryBudgetTokens"/> rather
/// than a partial overrun. Wire-supplied receipts, hashes, and session ids can only
/// ever <b>withhold</b> content, never widen access, so a forged or stale token is
/// fail-safe by construction.
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

    private static readonly IReadOnlyList<RepoContextReuseAck> NoReuse = Array.Empty<RepoContextReuseAck>();

    private readonly RepoContextSearchService _search;
    private readonly RepoContextGraphService _graph;
    private readonly RepoContextSessionStore _sessions;
    private readonly IGrainFactory _grainFactory;
    private readonly Orleans.Serialization.Serializer _serializer;
    private readonly IRepoContextTokenCounter _tokenCounter;
    private readonly IRepoContextUsageRecorder _usage;

    /// <summary>Creates the bundle service.</summary>
    /// <param name="search">The search service used to rank source for the task. Must not be <see langword="null"/>.</param>
    /// <param name="graph">The graph service whose outline projection backs outline detail. Must not be <see langword="null"/>.</param>
    /// <param name="sessions">The session store that persists per-session reuse bookkeeping. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to hydrate file bodies and token counts. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode stored records. Must not be <see langword="null"/>.</param>
    /// <param name="tokenCounter">The shared exact-BPE token counter used to measure and budget the bundle. Must not be <see langword="null"/>.</param>
    /// <param name="usage">The recorder that measures the usage figures of each answered call. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public RepoContextBundleService(
        RepoContextSearchService search,
        RepoContextGraphService graph,
        RepoContextSessionStore sessions,
        IGrainFactory grainFactory,
        Orleans.Serialization.Serializer serializer,
        IRepoContextTokenCounter tokenCounter,
        IRepoContextUsageRecorder usage)
    {
        ArgumentNullException.ThrowIfNull(search);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(sessions);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(tokenCounter);
        ArgumentNullException.ThrowIfNull(usage);

        _search = search;
        _graph = graph;
        _sessions = sessions;
        _grainFactory = grainFactory;
        _serializer = serializer;
        _tokenCounter = tokenCounter;
        _usage = usage;
    }

    /// <summary>
    /// Builds a budgeted context bundle for <paramref name="task"/> with no reuse
    /// inputs - equivalent to the reuse-aware overload with no <c>seen</c>,
    /// <c>known</c>, or <c>session</c>.
    /// </summary>
    /// <param name="repoId">The repository to bundle from. Must not be <see langword="null"/>.</param>
    /// <param name="task">The natural-language task to pack context for. Must not be <see langword="null"/>.</param>
    /// <param name="top">The maximum number of files to consider; clamped to [1, 50], defaulting to 10 for a non-positive value.</param>
    /// <param name="responseBudgetTokens">The hard token ceiling; clamped to [1, 200000], defaulting to 8192 for a non-positive value.</param>
    /// <param name="detail">The requested detail level; an unrecognised value resolves to auto.</param>
    /// <param name="cancellationToken">Cancels the bundle.</param>
    /// <returns>The packed bundle, whose total never exceeds the clamped budget.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public Task<RepoContextContextResult> BuildAsync(
        string repoId,
        string task,
        int top,
        int responseBudgetTokens,
        RepoContextContextDetail detail,
        CancellationToken cancellationToken)
        => BuildAsync(repoId, task, top, responseBudgetTokens, detail, seen: null, known: null, session: null, cancellationToken);

    /// <summary>
    /// Builds a budgeted context bundle for <paramref name="task"/>, suppressing any
    /// content the caller already holds. All numeric and enum inputs are clamped
    /// defensively, so a wire caller can never drive unbounded work:
    /// <paramref name="top"/> to [1, 50], <paramref name="responseBudgetTokens"/> to
    /// [1, 200000], and an unrecognised <paramref name="detail"/> to
    /// <see cref="RepoContextContextDetail.Auto"/>.
    /// </summary>
    /// <param name="repoId">The repository to bundle from. Must not be <see langword="null"/>.</param>
    /// <param name="task">The natural-language task to pack context for. Must not be <see langword="null"/>.</param>
    /// <param name="top">The maximum number of files to <b>deliver</b>; clamped to [1, 50], defaulting to 10. A fully-reused file never consumes one of these slots.</param>
    /// <param name="responseBudgetTokens">The hard token ceiling; clamped to [1, 200000], defaulting to 8192 for a non-positive value.</param>
    /// <param name="detail">The requested detail level; an unrecognised value resolves to auto.</param>
    /// <param name="seen">Opaque receipts of units the caller already holds; each matching unit is suppressed. May be <see langword="null"/> or empty.</param>
    /// <param name="known">Whole-file possession claims of the form <c>path@hash</c>; each is honoured only for a version this tool actually delivered whole to the same session. May be <see langword="null"/> or empty.</param>
    /// <param name="session">A named caller session whose recorded deliveries drive automatic unit suppression and validate <paramref name="known"/> claims, and into which this call's deliveries are recorded. May be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the bundle.</param>
    /// <returns>The packed bundle, whose total never exceeds the clamped budget.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    public async Task<RepoContextContextResult> BuildAsync(
        string repoId,
        string task,
        int top,
        int responseBudgetTokens,
        RepoContextContextDetail detail,
        IReadOnlyList<string>? seen,
        IReadOnlyList<string>? known,
        string? session,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(task);

        var clampedTop = ClampTop(top);
        var budget = ClampBudget(responseBudgetTokens);
        var floorLevel = detail == RepoContextContextDetail.Auto ? RepoContextContextDetail.Paths : detail;
        var sessionId = string.IsNullOrEmpty(session) ? null : session;

        var reuse = await BuildReuseContextAsync(repoId, seen, known, sessionId, cancellationToken).ConfigureAwait(false);

        // When reuse is engaged a fully-reused file must not consume a delivery slot, so
        // fetch a bounded backfill pool to still deliver up to `top` fresh files.
        var poolTop = reuse.Engaged ? MaxTop : clampedTop;
        var search = await _search.SearchAsync(repoId, task, poolTop, cancellationToken).ConfigureAwait(false);
        var pool = ResolveCandidates(search.Hits, poolTop);

        if (pool.Count == 0)
        {
            var empty = new RepoContextContextResult
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
                Session = sessionId,
                Reused = NoReuse,
            };

            // Side-effect-free recording: measure the answer, then return the exact same instance.
            _usage.Record(RepoContextUsageFigures.ForContextBundle(empty));
            return empty;
        }

        var levels = LevelsFor(detail);
        RepoContextBundlePacker.PackOutcome packed = default;
        var usedLevel = floorLevel;
        IReadOnlyList<RepoContextReuseAck> reused = NoReuse;

        foreach (var level in levels)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (reuse.Engaged)
            {
                var suppressed = await RenderAndSuppressAsync(repoId, pool, clampedTop, level, reuse, cancellationToken)
                    .ConfigureAwait(false);
                packed = RepoContextBundlePacker.Pack(suppressed.Candidates, budget, _tokenCounter);
                reused = suppressed.Reused;
            }
            else
            {
                var inputs = await RenderAsync(repoId, pool, clampedTop, level, cancellationToken).ConfigureAwait(false);
                packed = RepoContextBundlePacker.Pack(inputs, budget, _tokenCounter);
            }

            usedLevel = level;
            if (packed.Entries.Count > 0)
            {
                break;
            }
        }

        if (reuse.Engaged && sessionId is not null)
        {
            await RecordDeliveriesAsync(repoId, sessionId, packed.Entries, usedLevel, cancellationToken).ConfigureAwait(false);
        }

        var fitted = packed.Entries.Count > 0;
        var result = new RepoContextContextResult
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
            Session = sessionId,
            Reused = reused,
        };

        // Side-effect-free recording: measure the answer, then return the exact same instance.
        _usage.Record(RepoContextUsageFigures.ForContextBundle(result));
        return result;
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
    /// Assembles the per-call reuse context from the wire inputs and the session's
    /// recorded history. The unit-suppression set unions the caller's <c>seen</c>
    /// receipts with the session's recorded receipts; the whole-file possession oracle
    /// is exactly the session's recorded whole-file deliveries, against which each
    /// <c>known</c> claim is validated (the partial-to-whole guard).
    /// </summary>
    private async Task<ReuseContext> BuildReuseContextAsync(
        string repoId,
        IReadOnlyList<string>? seen,
        IReadOnlyList<string>? known,
        string? sessionId,
        CancellationToken cancellationToken)
    {
        var hasSeen = seen is { Count: > 0 };
        var hasKnown = known is { Count: > 0 };
        var engaged = hasSeen || hasKnown || sessionId is not null;
        if (!engaged)
        {
            return ReuseContext.Disabled;
        }

        var seenReceipts = new HashSet<string>(StringComparer.Ordinal);
        var possessionOracle = new HashSet<string>(StringComparer.Ordinal);

        if (hasSeen)
        {
            for (var i = 0; i < seen!.Count; i++)
            {
                var receipt = seen[i];
                if (!string.IsNullOrEmpty(receipt))
                {
                    seenReceipts.Add(receipt);
                }
            }
        }

        if (sessionId is not null)
        {
            var record = await _sessions.LoadAsync(repoId, sessionId, cancellationToken).ConfigureAwait(false);
            if (record is not null)
            {
                foreach (var element in record.Receipts.Values())
                {
                    seenReceipts.Add(Encoding.UTF8.GetString(element));
                }

                foreach (var element in record.Possession.Values())
                {
                    possessionOracle.Add(Encoding.UTF8.GetString(element));
                }
            }
        }

        // Validate each caller possession claim against the oracle. A claim is honoured
        // only when the referenced version was actually delivered whole to this session,
        // so partial evidence can never be promoted to a whole-file possession claim.
        HashSet<string>? claims = null;
        if (hasKnown)
        {
            for (var i = 0; i < known!.Count; i++)
            {
                if (RepoContextReuse.TryParseKnown(known[i], out var path, out var hash))
                {
                    var token = RepoContextReuse.PossessionToken(path, hash);
                    if (possessionOracle.Contains(token))
                    {
                        (claims ??= new HashSet<string>(StringComparer.Ordinal)).Add(token);
                    }
                }
            }
        }

        return new ReuseContext(seenReceipts, claims);
    }

    /// <summary>
    /// Records the units and whole-file possessions this call actually delivered into
    /// the session, so a later call in the same session need not pay for them again.
    /// Only a slices-detail span records possession (the guard on the write side); an
    /// outline or a path records unit receipts only.
    /// </summary>
    private async Task RecordDeliveriesAsync(
        string repoId,
        string sessionId,
        IReadOnlyList<RepoContextContextEntry> entries,
        RepoContextContextDetail level,
        CancellationToken cancellationToken)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var receipts = new List<string>();
        List<string>? possessions = null;

        for (var e = 0; e < entries.Count; e++)
        {
            var entry = entries[e];
            var units = entry.Units;
            for (var u = 0; u < units.Count; u++)
            {
                receipts.Add(units[u].Receipt);
            }

            // A whole-file possession is recorded only for a genuine whole-body (span)
            // delivery, never for an outline or path - this keeps a later `known` claim
            // honest.
            if (level == RepoContextContextDetail.Slices && entry.ContentHash is { } hash)
            {
                (possessions ??= new List<string>()).Add(RepoContextReuse.PossessionToken(entry.Path, hash));
            }
        }

        await _sessions.RecordAsync(
            repoId,
            sessionId,
            receipts,
            possessions ?? (IReadOnlyList<string>)Array.Empty<string>(),
            cancellationToken).ConfigureAwait(false);
    }

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
    /// Renders up to <paramref name="limit"/> candidates to their packed content at
    /// <paramref name="level"/> with no reuse suppression, caching each per-file read so
    /// an auto degrade across levels never re-reads the same record.
    /// </summary>
    private async Task<IReadOnlyList<RepoContextBundlePacker.Candidate>> RenderAsync(
        string repoId,
        List<PackCandidate> candidates,
        int limit,
        RepoContextContextDetail level,
        CancellationToken cancellationToken)
    {
        var take = Math.Min(limit, candidates.Count);
        var rendered = new List<RepoContextBundlePacker.Candidate>(take);
        for (var i = 0; i < take; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var candidate = candidates[i];

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
    /// Renders candidates at <paramref name="level"/> into reusable units, suppressing
    /// each unit the caller already holds and each whole file it validly possesses, and
    /// backfilling past fully-reused files so up to <paramref name="limit"/> fresh files
    /// are still delivered. Suppressed content is acknowledged, never packed, and never
    /// counted against the file or token budget.
    /// </summary>
    private async Task<SuppressionOutcome> RenderAndSuppressAsync(
        string repoId,
        List<PackCandidate> candidates,
        int limit,
        RepoContextContextDetail level,
        ReuseContext reuse,
        CancellationToken cancellationToken)
    {
        var packCandidates = new List<RepoContextBundlePacker.Candidate>(Math.Min(limit, candidates.Count));
        var acks = new List<RepoContextReuseAck>();

        for (var i = 0; i < candidates.Count && packCandidates.Count < limit; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var candidate = candidates[i];

            var body = await candidate.GetBodyAsync(repoId, _grainFactory, _serializer, cancellationToken)
                .ConfigureAwait(false);
            var contentHash = string.IsNullOrEmpty(body) ? null : RepoContextReuse.ContentHash(body!);

            // Without a body we cannot version the file, so no reuse tracking applies:
            // deliver it plainly at this level.
            if (contentHash is null)
            {
                packCandidates.Add(await RenderPlainAsync(repoId, candidate, level, cancellationToken).ConfigureAwait(false));
                continue;
            }

            // Whole-file suppression: a validated possession claim for the current
            // version makes any projection of the file redundant, at any detail level.
            if (reuse.OwnsWholeFile(candidate.Path, contentHash))
            {
                acks.Add(new RepoContextReuseAck
                {
                    Path = candidate.Path,
                    Kind = RepoContextReuse.FileKind,
                    Receipt = null,
                    ContentHash = contentHash,
                    Symbol = null,
                });
                continue;
            }

            var units = await BuildUnitsAsync(repoId, candidate, level, body, contentHash, cancellationToken)
                .ConfigureAwait(false);

            List<RepoContextContextUnit>? surviving = null;
            for (var u = 0; u < units.Count; u++)
            {
                var unit = units[u];
                if (reuse.OwnsUnit(unit.Receipt))
                {
                    acks.Add(new RepoContextReuseAck
                    {
                        Path = candidate.Path,
                        Kind = unit.Kind,
                        Receipt = unit.Receipt,
                        ContentHash = contentHash,
                        Symbol = unit.Symbol,
                    });
                }
                else
                {
                    (surviving ??= new List<RepoContextContextUnit>(units.Count)).Add(unit);
                }
            }

            // Every unit was already held: the file is fully reused and does not consume
            // a delivery slot.
            if (surviving is null)
            {
                continue;
            }

            var content = JoinUnits(surviving);
            var fullRead = await ResolveFullReadAsync(repoId, candidate, level, body, cancellationToken)
                .ConfigureAwait(false);

            packCandidates.Add(new RepoContextBundlePacker.Candidate(
                candidate.Path,
                candidate.Score,
                candidate.Reasons,
                content,
                fullRead,
                contentHash,
                surviving));
        }

        return new SuppressionOutcome(packCandidates, acks);
    }

    private async Task<RepoContextBundlePacker.Candidate> RenderPlainAsync(
        string repoId,
        PackCandidate candidate,
        RepoContextContextDetail level,
        CancellationToken cancellationToken)
    {
        string content;
        int? fullRead;
        switch (level)
        {
            case RepoContextContextDetail.Slices:
                var body = await candidate.GetBodyAsync(repoId, _grainFactory, _serializer, cancellationToken)
                    .ConfigureAwait(false);
                fullRead = await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                    .ConfigureAwait(false);
                content = string.IsNullOrEmpty(body) ? candidate.Path : body!;
                if (!string.IsNullOrEmpty(body))
                {
                    fullRead ??= _tokenCounter.CountTokens(body!);
                }

                break;

            case RepoContextContextDetail.Outline:
                var outline = await candidate.GetOutlineAsync(repoId, _graph, cancellationToken).ConfigureAwait(false);
                content = outline.Length == 0 ? candidate.Path : outline;
                fullRead = candidate.OutlineFullReadTokenCount;
                break;

            default:
                content = candidate.Path;
                fullRead = await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                    .ConfigureAwait(false);
                break;
        }

        return new RepoContextBundlePacker.Candidate(
            candidate.Path, candidate.Score, candidate.Reasons, content, fullRead);
    }

    /// <summary>
    /// Builds the reusable units for one file at <paramref name="level"/>: a single
    /// pointer unit (paths), a single span unit over the whole body (slices), or one
    /// outline unit per declared symbol (outline, falling back to a pointer when the
    /// file declares no symbols).
    /// </summary>
    private async Task<IReadOnlyList<RepoContextContextUnit>> BuildUnitsAsync(
        string repoId,
        PackCandidate candidate,
        RepoContextContextDetail level,
        string? body,
        string contentHash,
        CancellationToken cancellationToken)
    {
        switch (level)
        {
            case RepoContextContextDetail.Slices:
                if (string.IsNullOrEmpty(body))
                {
                    return [Pointer(repoId, candidate.Path, contentHash)];
                }

                return
                [
                    new RepoContextContextUnit
                    {
                        Receipt = RepoContextReuse.Receipt(repoId, candidate.Path, contentHash, RepoContextReuse.SpanKind, string.Empty),
                        Kind = RepoContextReuse.SpanKind,
                        Symbol = null,
                        TokenCount = _tokenCounter.CountTokens(body!),
                        Content = body!,
                    },
                ];

            case RepoContextContextDetail.Outline:
                var outline = await candidate.GetOutlineResultAsync(repoId, _graph, cancellationToken).ConfigureAwait(false);
                var symbols = outline.Symbols;
                if (symbols.Count == 0)
                {
                    return [Pointer(repoId, candidate.Path, contentHash)];
                }

                var units = new RepoContextContextUnit[symbols.Count];
                for (var s = 0; s < symbols.Count; s++)
                {
                    var symbol = symbols[s];
                    var line = symbol.Signature.Length != 0 ? symbol.Signature : symbol.FullyQualifiedName;
                    units[s] = new RepoContextContextUnit
                    {
                        Receipt = RepoContextReuse.Receipt(repoId, candidate.Path, contentHash, RepoContextReuse.OutlineKind, symbol.FullyQualifiedName),
                        Kind = RepoContextReuse.OutlineKind,
                        Symbol = symbol.FullyQualifiedName,
                        TokenCount = _tokenCounter.CountTokens(line),
                        Content = line,
                    };
                }

                return units;

            default:
                return [Pointer(repoId, candidate.Path, contentHash)];
        }
    }

    private RepoContextContextUnit Pointer(string repoId, string path, string contentHash) => new()
    {
        Receipt = RepoContextReuse.Receipt(repoId, path, contentHash, RepoContextReuse.PointerKind, string.Empty),
        Kind = RepoContextReuse.PointerKind,
        Symbol = null,
        TokenCount = _tokenCounter.CountTokens(path),
        Content = path,
    };

    private async ValueTask<int?> ResolveFullReadAsync(
        string repoId,
        PackCandidate candidate,
        RepoContextContextDetail level,
        string? body,
        CancellationToken cancellationToken)
    {
        switch (level)
        {
            case RepoContextContextDetail.Slices:
                var stored = await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                    .ConfigureAwait(false);
                if (stored is not null)
                {
                    return stored;
                }

                return string.IsNullOrEmpty(body) ? null : _tokenCounter.CountTokens(body!);

            case RepoContextContextDetail.Outline:
                return candidate.OutlineFullReadTokenCount;

            default:
                return await candidate.GetStoredTokenCountAsync(repoId, _grainFactory, _serializer, cancellationToken)
                    .ConfigureAwait(false);
        }
    }

    private static string JoinUnits(List<RepoContextContextUnit> units)
    {
        if (units.Count == 1)
        {
            return units[0].Content;
        }

        var builder = new StringBuilder();
        for (var i = 0; i < units.Count; i++)
        {
            if (i > 0)
            {
                builder.Append('\n');
            }

            builder.Append(units[i].Content);
        }

        return builder.ToString();
    }

    /// <summary>
    /// The immutable per-call reuse decision surface: the receipts the caller already
    /// holds (unit suppression) and the validated whole-file possession claims (whole
    /// -file suppression). Both are look-up only, so a wire-supplied token can only ever
    /// withhold content, never widen it.
    /// </summary>
    private sealed class ReuseContext
    {
        internal static readonly ReuseContext Disabled = new(null, null);

        private readonly HashSet<string>? _seenReceipts;
        private readonly HashSet<string>? _possessionClaims;

        internal ReuseContext(HashSet<string>? seenReceipts, HashSet<string>? possessionClaims)
        {
            _seenReceipts = seenReceipts;
            _possessionClaims = possessionClaims;
        }

        internal bool Engaged => _seenReceipts is not null;

        internal bool OwnsUnit(string receipt) => _seenReceipts is { } set && set.Contains(receipt);

        internal bool OwnsWholeFile(string path, string contentHash)
            => _possessionClaims is { } claims && claims.Contains(RepoContextReuse.PossessionToken(path, contentHash));
    }

    private readonly record struct SuppressionOutcome(
        IReadOnlyList<RepoContextBundlePacker.Candidate> Candidates,
        IReadOnlyList<RepoContextReuseAck> Reused);

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
        private RepoContextOutlineResult? _outlineResult;

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

            var outline = await GetOutlineResultAsync(repoId, graph, cancellationToken).ConfigureAwait(false);
            _outline = RenderOutline(outline);
            _outlineResolved = true;
            return _outline;
        }

        public async ValueTask<RepoContextOutlineResult> GetOutlineResultAsync(
            string repoId,
            RepoContextGraphService graph,
            CancellationToken cancellationToken)
        {
            if (_outlineResult is not null)
            {
                return _outlineResult;
            }

            var outline = await graph.OutlineAsync(repoId, Path, cancellationToken).ConfigureAwait(false);
            OutlineFullReadTokenCount = outline.FullReadTokenCount;
            _outlineResult = outline;
            return outline;
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
