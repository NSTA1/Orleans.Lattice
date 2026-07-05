namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Describes a retention-shape transition between two chronologically adjacent
/// revisions of the same key - the "retention changed here" boundary the History
/// tab renders as an inline divider. Derived purely from the per-row
/// <see cref="Orleans.Lattice.Api.State.RevisionRetention"/> descriptors already on the revisions, never
/// from a separate backend call. The boundary is attached to the first (newer)
/// revision whose retention differs from the older one.
/// </summary>
public readonly record struct RetentionTransition
{
    /// <summary>The retention mode of the older (preceding) revision.</summary>
    public HistoryRetentionMode From { get; init; }

    /// <summary>Whether the older revision retained its value bytes.</summary>
    public bool FromValueRetained { get; init; }

    /// <summary>The retention mode of the newer revision the divider sits at.</summary>
    public HistoryRetentionMode To { get; init; }

    /// <summary>Whether the newer revision retained its value bytes.</summary>
    public bool ToValueRetained { get; init; }

    /// <summary>
    /// A short human-readable label, for example
    /// <c>retention changed: full-value -&gt; metadata-only</c>.
    /// </summary>
    public string Label() => $"retention changed: {Describe(From, FromValueRetained)} -> {Describe(To, ToValueRetained)}";

    /// <summary>The display name of a retention shape, accounting for an aged-out hybrid row.</summary>
    public static string Describe(HistoryRetentionMode mode, bool valueRetained) => mode switch
    {
        HistoryRetentionMode.FullValue => "full-value",
        HistoryRetentionMode.MetadataOnly => "metadata-only",
        HistoryRetentionMode.Hybrid => valueRetained ? "hybrid (value)" : "hybrid (metadata)",
        _ => mode.ToString(),
    };
}
