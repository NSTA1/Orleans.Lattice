namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Holds the current advisory <see cref="ExplorerCapabilities"/> for the session
/// and notifies the shell when it changes. The capability map is probed once
/// after sign-in / reconnect and cached here so the shell can gate areas and
/// actions without re-probing per render. It is a UX affordance only - the
/// server stays the fail-closed enforcement point.
/// </summary>
public interface IExplorerCapabilityStore
{
    /// <summary>The current capability map. Never <see langword="null"/>; starts at <see cref="ExplorerCapabilities.Empty"/>.</summary>
    ExplorerCapabilities Current { get; }

    /// <summary>Raised after <see cref="Set"/> or <see cref="Reset"/> changes the map.</summary>
    event Action? Changed;

    /// <summary>
    /// Replaces the current map with <paramref name="capabilities"/> and raises
    /// <see cref="Changed"/>.
    /// </summary>
    /// <param name="capabilities">The new capability map. Must not be <see langword="null"/>.</param>
    void Set(ExplorerCapabilities capabilities);

    /// <summary>
    /// Resets the map to <see cref="ExplorerCapabilities.Empty"/> (deny-all) and
    /// raises <see cref="Changed"/>. Call on sign-out or before re-probing.
    /// </summary>
    void Reset();
}
