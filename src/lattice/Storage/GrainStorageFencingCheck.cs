using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// Runs <see cref="GrainStorageFencingProbe"/> against the grain storage provider
/// registered under <see cref="LatticeOptions.StorageProviderName"/> once, as the
/// silo becomes active, and acts on the verdict according to
/// <see cref="LatticeGrainStorageFencingOptions.Mode"/>.
/// </summary>
/// <remarks>
/// The probe runs at <see cref="ServiceLifecycleStage.Active"/> rather than from a
/// hosted service because providers such as Orleans' memory storage write through
/// grains, which only accept calls once the silo is active, and because a hosted
/// service's start order relative to the silo depends on registration order. A
/// failure in <see cref="LatticeGrainStorageFencingMode.Reject"/> mode throws from
/// that stage, which fails silo start. When the mode is
/// <see cref="LatticeGrainStorageFencingMode.Disabled"/> the check subscribes to
/// nothing, so it performs no storage call.
/// </remarks>
internal sealed class GrainStorageFencingCheck(
    IServiceProvider services,
    IOptions<LatticeGrainStorageFencingOptions> options,
    ILogger<GrainStorageFencingCheck> logger) : ILifecycleParticipant<ISiloLifecycle>
{
    /// <summary>
    /// The verdict of the probe this silo ran, or <see langword="null"/> when it
    /// has not run (not yet started, or disabled).
    /// </summary>
    internal GrainStorageFencingProbeResult? LastResult { get; private set; }

    /// <inheritdoc />
    public void Participate(ISiloLifecycle lifecycle)
    {
        var o = options.Value;
        if (o.Mode == LatticeGrainStorageFencingMode.Disabled)
        {
            logger.LogInformation(
                "Lattice grain-storage fencing check: Mode={Mode}. The probe that verifies the '{Provider}' grain "
                    + "storage provider rejects writes carrying a stale ETag will not run; Lattice requires that "
                    + "provider to enforce ETags.",
                o.Mode,
                LatticeOptions.StorageProviderName);
            return;
        }

        lifecycle.Subscribe(
            nameof(GrainStorageFencingCheck),
            ServiceLifecycleStage.Active,
            RunAsync,
            static _ => Task.CompletedTask);
    }

    /// <summary>
    /// Runs the probe, logs the verdict, and throws when the provider is unfenced
    /// and the mode is <see cref="LatticeGrainStorageFencingMode.Reject"/>.
    /// </summary>
    internal async Task RunAsync(CancellationToken cancellationToken)
    {
        var o = options.Value;
        var result = await ProbeAsync(o.ProbeTimeout, cancellationToken).ConfigureAwait(false);
        LastResult = result;

        switch (result.Verdict)
        {
            case GrainStorageFencingVerdict.Fenced:
                logger.LogInformation(
                    "Lattice grain-storage fencing check: Mode={Mode}, Verdict={Verdict}: {Reason}.",
                    o.Mode,
                    result.Verdict,
                    result.Reason);
                return;

            case GrainStorageFencingVerdict.Inconclusive:
                logger.LogWarning(
                    result.Fault,
                    "Lattice grain-storage fencing check: Mode={Mode}, Verdict={Verdict}: {Reason}. Lattice requires the "
                        + "'{Provider}' grain storage provider to reject a write carrying a stale ETag with "
                        + "InconsistentStateException; this silo could not confirm that it does.",
                    o.Mode,
                    result.Verdict,
                    result.Reason,
                    LatticeOptions.StorageProviderName);
                return;
        }

        var message =
            $"Lattice grain-storage fencing check: the '{LatticeOptions.StorageProviderName}' grain storage provider "
            + "accepted a write carrying a stale ETag. Lattice requires that provider to enforce ETags on write: it "
            + "writes some grain state directly through the provider, and it relies on the provider rejecting a stale "
            + "duplicate activation's write. Without that, an older checkpoint can overwrite a newer one and the "
            + "write-ahead log can be trimmed past entries a later rebuild needs. Use a provider that enforces ETags "
            + "(for example Orleans' memory, Azure Table, Azure Blob, Cosmos DB or ADO.NET storage).";

        if (o.Mode == LatticeGrainStorageFencingMode.Reject)
        {
            logger.LogError("{Message} Mode=Reject, so silo start fails.", message);
            throw new OrleansConfigurationException(message);
        }

        logger.LogWarning(
            "{Message} Mode={Mode}; set LatticeGrainStorageFencingOptions.Mode to Reject to fail silo start instead.",
            message,
            o.Mode);
    }

    private async Task<GrainStorageFencingProbeResult> ProbeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var storage = services.GetKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
        if (storage is null)
        {
            return new GrainStorageFencingProbeResult(
                GrainStorageFencingVerdict.Inconclusive,
                $"no grain storage provider is registered under the name '{LatticeOptions.StorageProviderName}'");
        }

        try
        {
            return await GrainStorageFencingProbe.RunAsync(storage)
                .WaitAsync(timeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException ex)
        {
            return new GrainStorageFencingProbeResult(
                GrainStorageFencingVerdict.Inconclusive,
                $"the probe did not finish within {timeout}",
                ex);
        }
    }
}
