using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The export sources a bUnit test controls: the probe and every download read the same
/// in-memory <see cref="Source"/>, and the connection each download was prepared with is
/// recorded.
/// </summary>
internal sealed class FakeDataExportSources : IDataExportSources
{
    /// <summary>The in-memory Data API every source reads.</summary>
    public FakeDataExportSource Source { get; } = new();

    /// <summary>Fails the probe with this exception, when set.</summary>
    public Exception? ProbeFault { get; set; }

    /// <summary>The connections downloads were prepared with.</summary>
    public List<LatticeConnectionSettings> Prepared { get; } = [];

    /// <inheritdoc />
    public IDataExportSource OpenForCircuit() =>
        ProbeFault is { } fault ? throw fault : new Borrowed(Source);

    /// <inheritdoc />
    public Func<IDataExportSource> ForDownload(LatticeConnectionSettings settings)
    {
        Prepared.Add(settings);
        return () => new Borrowed(Source);
    }

    private sealed class Borrowed(FakeDataExportSource inner) : IDataExportSource
    {
        public Task<Orleans.Lattice.Api.Data.DataRangePage> ReadAsync(Orleans.Lattice.Api.Data.DataRangeRequest request, CancellationToken cancellationToken) =>
            inner.ReadAsync(request, cancellationToken);

        public void Dispose()
        {
        }
    }
}
