namespace Orleans.Lattice.Explorer.UI.Navigation.Completion;

/// <summary>One source's answer to a completion request.</summary>
/// <param name="Source">The source that answered.</param>
/// <param name="Completions">Its completions, at most <see cref="AddressQuery.MaximumResults"/>; empty unless it completed.</param>
/// <param name="Outcome">Whether it answered in time, timed out, or failed.</param>
internal sealed record AddressCompletionBatch(
    AddressCompletionSourceEntry Source,
    IReadOnlyList<AddressCompletion> Completions,
    AddressCompletionOutcome Outcome);
