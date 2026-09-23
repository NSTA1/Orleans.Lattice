namespace Orleans.Lattice.Testing;

/// <summary>
/// Product-agnostic shape of one WAL entry as
/// <see cref="WalStorageProviderContractTestsBase"/> sees it: the offset the
/// caller assigned and the key and value payload the entry carries. The shared
/// testing library references no Orleans.Lattice assembly, so each provider's
/// probe maps this to and from the provider's own entry type.
/// </summary>
/// <param name="Offset">Caller-assigned WAL offset.</param>
/// <param name="Key">The mutation key.</param>
/// <param name="Value">The mutation value bytes.</param>
public sealed record WalContractEntry(long Offset, string Key, byte[] Value)
{
    /// <summary>
    /// A value-shaped rendering (offset, key, hex value). Record equality
    /// compares <see cref="Value"/> by reference, so assertions compare these
    /// strings instead.
    /// </summary>
    public override string ToString() => $"{Offset}:{Key}:{Convert.ToHexString(Value)}";
}
