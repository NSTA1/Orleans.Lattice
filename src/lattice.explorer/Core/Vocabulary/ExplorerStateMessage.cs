namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// What a surface says when it has nothing to show: what happened, and what to
/// do about it.
/// </summary>
/// <remarks>
/// <para>
/// The shape mirrors the help primitive deliberately, so a message can be
/// rendered as a panel or as a disclosure without rewording:
/// <see cref="Explanation"/> maps to the primitive's explanation,
/// <see cref="Remedy"/> and <see cref="RemedyLabel"/> to its remedy block, and
/// <see cref="IsDenial"/> to its denial tone.
/// </para>
/// <para>
/// Every message is built once, by <see cref="ExplorerStateCopy"/>, and read
/// many times.
/// </para>
/// </remarks>
public sealed record ExplorerStateMessage
{
    /// <summary>Which state this describes.</summary>
    public required ExplorerStateKind Kind { get; init; }

    /// <summary>
    /// The one-line summary, for a heading: <c>No trees yet</c>. No trailing
    /// full stop.
    /// </summary>
    public required string Headline { get; init; }

    /// <summary>
    /// What actually happened, in complete sentences - in particular, whether
    /// the list is empty because there is nothing there, because a scope is
    /// filtering it, or because the caller may not read it.
    /// </summary>
    public required string Explanation { get; init; }

    /// <summary>
    /// What the reader can do next, or <see langword="null"/> when there is
    /// nothing useful to suggest (a load in flight, for example).
    /// </summary>
    public string? Remedy { get; init; }

    /// <summary>
    /// The label introducing <see cref="Remedy"/>. Defaults to the settled
    /// wording so a remedy reads the same wherever it is rendered.
    /// </summary>
    public string RemedyLabel { get; init; } = ExplorerVocabulary.RemedyLabel;

    /// <summary>
    /// The label for the single action that resolves the state - <c>Try again</c>,
    /// <c>Sign in</c>, <c>Show all tenants</c> - or <see langword="null"/> when
    /// no one action does.
    /// </summary>
    public string? ActionLabel { get; init; }

    /// <summary>
    /// The glossary term that explains the concept the message is about, or
    /// <see langword="null"/>.
    /// </summary>
    public string? TermId { get; init; }

    /// <summary>Where to read more, or <see langword="null"/>.</summary>
    public string? DocsLink { get; init; }

    /// <summary>
    /// Whether this is a refusal aimed at the caller, and should therefore be
    /// rendered in the help primitive's denial tone.
    /// </summary>
    /// <remarks>
    /// <see cref="ExplorerStateKind.Unavailable"/> is deliberately not a denial:
    /// nothing is being withheld from the caller, the cluster simply does not
    /// run the feature.
    /// </remarks>
    public bool IsDenial => Kind is ExplorerStateKind.NotPermitted or ExplorerStateKind.SignInRequired;

    /// <summary>Whether this describes a read still in flight.</summary>
    public bool IsBusy => Kind == ExplorerStateKind.Loading;
}
