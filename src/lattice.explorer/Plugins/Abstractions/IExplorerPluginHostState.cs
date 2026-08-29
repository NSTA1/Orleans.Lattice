namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The ambient Explorer state the host publishes to plugins: the current
/// selection, the connection health, and the resolved tenant scope.
/// <para>
/// This is the shell's side of the host-context seam. The shell implements it
/// by adapting the Explorer's own services onto these narrow projections, and
/// the contract hands each plugin a bound
/// <see cref="IExplorerPluginHostContext"/> over it. Keeping the adaptation on
/// the shell's side is what lets this contract carry no cluster dependency at
/// all: a plugin cannot be handed the connection, because nothing here can
/// express it.
/// </para>
/// </summary>
public interface IExplorerPluginHostState
{
    /// <summary>
    /// The currently selected tree or view, or <see langword="null"/> when
    /// none is selected.
    /// </summary>
    ExplorerPluginSelection? Selection { get; }

    /// <summary>The current connection health.</summary>
    ExplorerPluginConnectionStatus Connection { get; }

    /// <summary>The active tenant and the host-resolved effective visibility.</summary>
    ExplorerPluginTenantScope Tenant { get; }

    /// <summary>
    /// Raised after one of the ambient facts changes, carrying which one so a
    /// subscriber can ignore transitions it does not read.
    /// </summary>
    event Action<ExplorerPluginHostChange>? Changed;
}
