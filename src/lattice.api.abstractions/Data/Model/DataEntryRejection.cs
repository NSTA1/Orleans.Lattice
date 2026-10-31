namespace Orleans.Lattice.Api.Data;

/// <summary>
/// One entry a streamed write refused, as reported in
/// <see cref="DataSetManyStreamBatch.Rejections"/>: where it was in the stream, its key,
/// and why it was refused. A rejection never carries the value.
/// </summary>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataEntryRejection)]
[Immutable]
public sealed record DataEntryRejection
{
    /// <summary>The refusal code when the caller's access does not allow writing the key.</summary>
    public const string DeniedCode = "denied";

    /// <summary>The refusal code when the tree's schema policy rejected the value.</summary>
    public const string SchemaCode = "schema";

    /// <summary>The refusal code when the entry itself is not a valid write (for example an empty key).</summary>
    public const string InvalidCode = "invalid";

    /// <summary>
    /// The zero-based position of the entry in the stream, counted from
    /// <see cref="DataSetManyStreamOptions.FirstOrdinal"/>.
    /// </summary>
    [Id(0)] public required long Ordinal { get; init; }

    /// <summary>The refused entry's key.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>Why the entry was refused: <see cref="DeniedCode"/>, <see cref="SchemaCode"/> or <see cref="InvalidCode"/>.</summary>
    [Id(2)] public required string Code { get; init; }

    /// <summary>A plain, one-line reason a person can act on.</summary>
    [Id(3)] public required string Reason { get; init; }
}
