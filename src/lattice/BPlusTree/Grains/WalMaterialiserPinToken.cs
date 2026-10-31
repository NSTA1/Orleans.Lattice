namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A retirement token taken from one <see cref="IWalMaterialiserPinGrain"/>
/// before a WAL GC retirement reads the pins it holds (issue #5358). A removal
/// presented with it through
/// <see cref="IWalMaterialiserPinGrain.RemoveIfUnmergedSinceAsync"/> is refused
/// when the consumer was merged after the token was taken, or when the pin
/// grain's activation is no longer the one that issued it.
/// </summary>
/// <remarks>
/// Both fields describe the issuing activation's memory only, so the token is
/// worthless across activations by design: the pin grain refuses it rather
/// than guessing whether a merge it no longer remembers happened.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalMaterialiserPinToken)]
internal readonly record struct WalMaterialiserPinToken
{
    /// <summary>Identifies the pin grain activation that issued the token.</summary>
    [Id(0)]
    public Guid Epoch { get; init; }

    /// <summary>The issuing activation's merge sequence when the token was taken.</summary>
    [Id(1)]
    public long Sequence { get; init; }

    /// <summary>Creates a token.</summary>
    /// <param name="epoch">The issuing activation's epoch.</param>
    /// <param name="sequence">The issuing activation's merge sequence.</param>
    public WalMaterialiserPinToken(Guid epoch, long sequence)
    {
        Epoch = epoch;
        Sequence = sequence;
    }
}
