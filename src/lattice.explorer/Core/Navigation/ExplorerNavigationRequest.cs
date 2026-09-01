namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// A request from the shell router to the head's framework binding: put
/// <see cref="Address"/> in the address bar.
/// </summary>
/// <remarks>
/// The router owns the shell's route model but not the browser. It raises this
/// request and the head's binding performs the actual navigation, which is what
/// keeps the router free of any framework dependency and unit-testable without a
/// renderer.
/// </remarks>
/// <param name="Address">The root-relative address to navigate to, already canonical.</param>
/// <param name="Replace">
/// Whether to replace the current history entry instead of pushing a new one.
/// Replace is for corrections the user did not ask for - canonicalising a
/// mis-cased link, or landing a restored view on a bare <c>/</c> - so that
/// pressing Back does not walk through the shell's own bookkeeping.
/// </param>
public readonly record struct ExplorerNavigationRequest(string Address, bool Replace);
