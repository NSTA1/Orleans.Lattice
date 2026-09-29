using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>A completion request from the address line.</summary>
/// <param name="Text">
/// What the user typed, with the mode's prefix removed: for <c>a/cr</c> it is
/// <c>cr</c>, for <c>orders</c> it is <c>orders</c>. For
/// <see cref="AddressQueryMode.Address"/> it is the raw input.
/// </param>
/// <param name="Mode">What kind of input this is.</param>
/// <param name="Current">Where the user is now.</param>
internal sealed record AddressQuery(string Text, AddressQueryMode Mode, ExplorerAddress Current)
{
    /// <summary>The most results one source may contribute.</summary>
    public const int MaximumResults = 20;

    /// <summary>The most results this request wants from one source.</summary>
    public int Limit => MaximumResults;
}
