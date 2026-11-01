using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Wraps an <see cref="IGrainStorage"/> and logs every failed write at Error
/// before rethrowing, the way Orleans' Azure Table grain storage does (event
/// 200107, "Error Writing: GrainType=..."). Used to prove that the fencing
/// probe's deliberate stale write leaves no Error in a healthy silo's log
/// (issue #5442).
/// </summary>
internal sealed class ErrorLoggingGrainStorage(IGrainStorage inner, ILogger<ErrorLoggingGrainStorage> logger) : IGrainStorage
{
    /// <summary>The event id the provider logs a failed write under.</summary>
    internal static readonly EventId WriteError = new(200107, "WriteError");

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => inner.ReadStateAsync(stateName, grainId, grainState);

    /// <inheritdoc />
    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        try
        {
            await inner.WriteStateAsync(stateName, grainId, grainState).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(WriteError, ex, "Error Writing: GrainType={GrainType} GrainId={GrainId}", stateName, grainId);
            throw;
        }
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => inner.ClearStateAsync(stateName, grainId, grainState);
}
