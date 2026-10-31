using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Opens the Data API sources the export reads: one over the circuit's own channel,
/// for the capability probe the page runs, and one over a channel of the download's
/// own, for the export itself.
/// </summary>
internal interface IDataExportSources
{
    /// <summary>A source over the circuit's channel, for a read the circuit makes now.</summary>
    /// <returns>The source; disposing it leaves the circuit's channel open.</returns>
    IDataExportSource OpenForCircuit();

    /// <summary>
    /// A factory that opens a source over <paramref name="settings"/> when the download starts.
    /// It captures values and singletons only, because the download runs outside the circuit.
    /// </summary>
    /// <param name="settings">The connection snapshot the circuit captured.</param>
    /// <returns>The factory.</returns>
    Func<IDataExportSource> ForDownload(LatticeConnectionSettings settings);
}
