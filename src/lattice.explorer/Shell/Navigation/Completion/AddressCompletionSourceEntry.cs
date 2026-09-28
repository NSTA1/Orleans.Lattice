namespace Orleans.Lattice.Explorer.Shell.Navigation.Completion;

/// <summary>A completion source to ask, and the name its results are grouped under.</summary>
/// <param name="Key">A stable key for the group, such as the area key.</param>
/// <param name="Name">The group's visible name, such as the area's display name.</param>
/// <param name="Source">The source.</param>
internal sealed record AddressCompletionSourceEntry(string Key, string Name, IAddressCompletionSource Source);
