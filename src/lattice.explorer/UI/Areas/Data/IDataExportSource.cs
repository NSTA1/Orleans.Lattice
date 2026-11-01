using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Where a data export reads its pages: one Data API range read at a time, as the
/// user the export runs for. Owned by one export and disposed with it.
/// </summary>
internal interface IDataExportSource : IDisposable
{
    /// <summary>Reads one page of a range.</summary>
    /// <param name="request">The range request.</param>
    /// <param name="cancellationToken">Stops the read.</param>
    /// <returns>The page.</returns>
    /// <exception cref="DataExportCursorExpiredException">The request resumed a scan whose cursor has expired.</exception>
    Task<DataRangePage> ReadAsync(DataRangeRequest request, CancellationToken cancellationToken);
}
