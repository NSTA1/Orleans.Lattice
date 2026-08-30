namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pure, deterministic packing core behind the <c>repocontext_context</c> tool.
/// Given ranked candidates already rendered to their packed content, it greedily
/// admits them in rank order under a <b>hard</b> token ceiling and reports the exact
/// packed content total, the estimated wire cost of the response, whether anything was
/// dropped, and the cheapest single candidate so a non-fitting bundle can hand back a
/// guaranteed-to-fit retry budget.
/// <para>
/// <b>The ceiling bounds the response, not merely the content.</b> Each candidate is
/// charged its delivered content <i>plus</i> its own JSON envelope (path, reasons,
/// content hash, per-unit receipts) <i>times</i> the SDK's dual-emission factor, because
/// the MCP SDK serializes every result twice. Charging content alone lets a bundle that
/// reports a few thousand tokens land as a response many times that size, which is the
/// defect this model exists to prevent (issue #1811). See
/// <see cref="RepoContextResponseCost"/>.
/// </para>
/// <para>
/// The pack is a single forward pass with no sorting, no LINQ, and no per-token
/// materialisation: the only heap allocation is the result list, pre-sized to the
/// candidate count. Determinism is structural - the same candidates and ceiling
/// always yield the same bundle, independent of timing or hashing order.
/// </para>
/// </summary>
internal static class RepoContextBundlePacker
{
    /// <summary>
    /// One ranked candidate ready to pack: its identity and search provenance plus
    /// the content already rendered at the target detail level. The packer measures
    /// <see cref="Content"/> with the token counter; it never re-renders it.
    /// </summary>
    /// <param name="Path">The repository-relative path of the file.</param>
    /// <param name="Score">The search score that ranked the file.</param>
    /// <param name="Reasons">The machine-readable match reasons from the search hit.</param>
    /// <param name="Content">The content already rendered at the target detail level.</param>
    /// <param name="FullReadTokenCount">The whole-file read cost, or <see langword="null"/> when unknown.</param>
    /// <param name="ContentHash">The packed file version's content hash, or <see langword="null"/> when reuse tracking did not apply.</param>
    /// <param name="Units">The surviving rendered units backing <paramref name="Content"/>; empty when reuse tracking did not apply.</param>
    internal readonly record struct Candidate(
        string Path,
        double Score,
        IReadOnlyList<string> Reasons,
        string Content,
        int? FullReadTokenCount,
        string? ContentHash = null,
        IReadOnlyList<RepoContextRenderedUnit>? Units = null);

    /// <summary>
    /// The outcome of a pack: the admitted entries, their exact content-token sum, the
    /// estimated wire cost of the whole response, whether any candidate was dropped for
    /// not fitting, and the smallest single-candidate <b>response</b> cost seen (0 when
    /// there were no candidates), which is the guaranteed-to-fit retry budget when
    /// <see cref="Entries"/> is empty.
    /// </summary>
    /// <param name="Entries">The admitted entries in rank order.</param>
    /// <param name="TotalTokens">The exact BPE sum of the admitted entries' delivered content.</param>
    /// <param name="ResponseTokens">The estimated wire cost of the response, never exceeding the budget.</param>
    /// <param name="Truncated">Whether at least one candidate was dropped for not fitting.</param>
    /// <param name="MinCandidateTokens">The cheapest single-candidate response cost, or 0 when there were none.</param>
    internal readonly record struct PackOutcome(
        IReadOnlyList<RepoContextContextEntry> Entries,
        int TotalTokens,
        int ResponseTokens,
        bool Truncated,
        int MinCandidateTokens);

    /// <summary>
    /// Packs <paramref name="candidates"/> in order under <paramref name="budgetTokens"/>,
    /// admitting each whose estimated <b>response</b> cost fits the remaining budget and
    /// dropping (not reordering) the rest.
    /// </summary>
    /// <param name="candidates">The ranked candidates, already rendered to content. Must not be <see langword="null"/>.</param>
    /// <param name="budgetTokens">The hard token ceiling on the response. A non-positive value admits nothing.</param>
    /// <param name="counter">The exact-BPE token counter used to measure each candidate. Must not be <see langword="null"/>.</param>
    /// <returns>The pack outcome, whose <see cref="PackOutcome.ResponseTokens"/> never exceeds <paramref name="budgetTokens"/>.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    internal static PackOutcome Pack(
        IReadOnlyList<Candidate> candidates,
        int budgetTokens,
        IRepoContextTokenCounter counter)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(counter);

        var entries = new List<RepoContextContextEntry>(candidates.Count);
        var contentTotal = 0;
        var truncated = false;
        var minCost = int.MaxValue;

        // The bundle's own top-level scaffolding is charged once, up front, so admitted
        // entries are measured against the headroom that actually remains.
        var scaffold = RepoContextResponseCost.WithDualEmission(RepoContextResponseCost.BundleScaffoldTokens);
        var responseTotal = scaffold;

        // Index the source list directly rather than enumerating it, so a struct list
        // does not box an enumerator and the loop stays allocation-free past the result.
        for (var i = 0; i < candidates.Count; i++)
        {
            var candidate = candidates[i];
            var units = candidate.Units ?? Array.Empty<RepoContextRenderedUnit>();
            var contentCost = counter.CountTokens(candidate.Content);
            var cost = RepoContextResponseCost.WithDualEmission(
                RepoContextResponseCost.EntryEnvelopeTokens(
                    contentCost, candidate.Path, candidate.Reasons, candidate.ContentHash, units, counter));

            // The retry budget must admit the entry *and* the bundle scaffolding it
            // ships inside, or a caller retrying at exactly this figure fails again.
            var retryCost = cost + scaffold;
            if (retryCost < minCost)
            {
                minCost = retryCost;
            }

            // Compare against the remaining headroom (budget - total) rather than
            // total + cost, so a large cost cannot overflow the running sum.
            if (budgetTokens > 0 && cost <= budgetTokens - responseTotal)
            {
                entries.Add(new RepoContextContextEntry
                {
                    Path = candidate.Path,
                    Score = candidate.Score,
                    Reasons = candidate.Reasons,
                    TokenCount = contentCost,
                    FullReadTokenCount = candidate.FullReadTokenCount,
                    Content = candidate.Content,
                    ContentHash = candidate.ContentHash,
                    Units = ToWire(units),
                });
                contentTotal += contentCost;
                responseTotal += cost;
            }
            else
            {
                truncated = true;
            }
        }

        return new PackOutcome(
            entries,
            contentTotal,
            entries.Count == 0 ? 0 : responseTotal,
            truncated,
            candidates.Count == 0 ? 0 : minCost);
    }

    /// <summary>
    /// Projects the rendered units to their public wire descriptors, dropping the
    /// rendered text the owning entry's content already carries.
    /// </summary>
    private static IReadOnlyList<RepoContextContextUnit> ToWire(IReadOnlyList<RepoContextRenderedUnit> units)
    {
        if (units.Count == 0)
        {
            return Array.Empty<RepoContextContextUnit>();
        }

        var wire = new RepoContextContextUnit[units.Count];
        for (var i = 0; i < units.Count; i++)
        {
            wire[i] = units[i].ToWire();
        }

        return wire;
    }
}
