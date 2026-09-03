namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One bounded batch of a shard-level emptiness probe (see
/// <see cref="Grains.IShardRootGrain.AnyBoundedAsync"/>).
/// <para>
/// <see cref="Found"/> is a positive answer and terminates the walk
/// immediately: once one live key has been seen the shard is non-empty and no
/// further batch is needed. A batch that found nothing is <b>not</b> an answer
/// unless <see cref="ResumeFromInclusive"/> is <see langword="null"/>, which is
/// the only state that means "and there is no more chain to look at".
/// </para>
/// <para>
/// The resume position is a <b>key</b>, never a leaf grain id, for the reason
/// set out on <see cref="ShardCountPage"/>: a reclaimed leaf virtual-activates
/// empty and would end the walk early, which here would report a populated
/// shard as empty (issue 1955).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardAnyPage)]
[Immutable]
internal readonly record struct ShardAnyPage
{
    /// <summary>Whether a live key was found within this batch.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when the chain is
    /// exhausted. Always <see langword="null"/> when <see cref="Found"/> is
    /// <see langword="true"/>, because the walk is over.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
