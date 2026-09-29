using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>One suggestion in the address line: a place to go, and how to name it.</summary>
/// <param name="Label">The suggestion's text, such as the tree id <c>a/crm/orders</c>. Shown in Cascadia Mono.</param>
/// <param name="Target">Where choosing it navigates.</param>
/// <param name="Detail">An optional plain description, such as "tree, 1,204 keys".</param>
internal sealed record AddressCompletion(string Label, ExplorerAddress Target, string? Detail = null);
