using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>Where the browser should be for an address it arrived at.</summary>
/// <param name="Address">The canonical address to show.</param>
/// <param name="RedirectTo">The address to replace the browser's with, or <see langword="null"/> when it is already canonical.</param>
/// <param name="Notice">A sentence to announce, such as a refused tenant switch, or <see langword="null"/>.</param>
internal sealed record ExplorerAddressResolution(ExplorerAddress Address, ExplorerAddress? RedirectTo, string? Notice);
