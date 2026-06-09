namespace Orleans.Lattice;

/// <summary>
/// Tunables for <see cref="ILatticeAdmin.ExecuteWalMoveAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveOptions)]
[Immutable]
public readonly record struct WalMoveOptions
{
    /// <summary>Default quiesce lease (30 seconds).</summary>
    public static readonly TimeSpan DefaultQuiesceLease = TimeSpan.FromSeconds(30);

    /// <summary>Default copy page size (256 entries).</summary>
    public const int DefaultCopyPageSize = 256;

    /// <summary>
    /// How long the source partition stays fenced while the move copies its
    /// tail and flips the placement pin. If the move coordinator fails, the
    /// fence self-heals after this lease so the partition resumes service.
    /// Defaults to <see cref="DefaultQuiesceLease"/> when unset (zero).
    /// </summary>
    [Id(0)] public TimeSpan QuiesceLease { get; init; }

    /// <summary>
    /// Number of entries copied per page from source to target. Defaults to
    /// <see cref="DefaultCopyPageSize"/> when unset (zero or negative).
    /// </summary>
    [Id(1)] public int CopyPageSize { get; init; }

    /// <summary>
    /// When <see langword="true"/> (the default), the move verifies the target
    /// tail matches the copied source range before flipping the placement pin.
    /// </summary>
    [Id(2)] public bool VerifyAfterCopy { get; init; }

    /// <summary>
    /// The conventional default options: 30 s lease, 256-entry pages, verify on.
    /// </summary>
    public static WalMoveOptions Default => new()
    {
        QuiesceLease = DefaultQuiesceLease,
        CopyPageSize = DefaultCopyPageSize,
        VerifyAfterCopy = true,
    };

    /// <summary>The effective quiesce lease, substituting the default for an unset value.</summary>
    public TimeSpan EffectiveQuiesceLease => QuiesceLease > TimeSpan.Zero ? QuiesceLease : DefaultQuiesceLease;

    /// <summary>The effective copy page size, substituting the default for an unset value.</summary>
    public int EffectiveCopyPageSize => CopyPageSize > 0 ? CopyPageSize : DefaultCopyPageSize;
}
