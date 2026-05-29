// Benchmark-only null IGrainStorage.
//
// `Orleans.Persistence.Memory` (the package the bench has been using as
// the leaf/internal-grain backing store) ships with
// `MemoryGrainStorageOptions.NumStorageGrains = 10` by default - i.e.
// the entire silo funnels every `IPersistentState.WriteStateAsync` call
// through ten `memorystorage/N` activations. Under the post-step-8c-c-i
// WAL pipeline (8 shards x 8 in-flight appends per shard) the
// foreground leaf path issues dozens of concurrent WriteStateAsync
// calls, and the Orleans placement directory starts rejecting the
// resulting forwarded messages with
//   "Forwarding failed: ... Unable to create local activation. Rejecting now."
// We saw 2074 such rejections in the step 8c-c-i silo log, and the
// caller-visible throughput collapsed from 1.6 k/s to 67/s.
//
// `NullGrainStorage` is a no-op IGrainStorage: WriteStateAsync /
// ReadStateAsync / ClearStateAsync all return `Task.CompletedTask`
// without touching any backing grain. This is **only** safe for the
// throughput benchmark because:
//
// 1. The lattice's correctness contract is "in-memory state survives
//    grain re-activation iff the IPersistentState has persisted the
//    write". The benchmark never re-activates a grain mid-run, so
//    no read-after-write semantics are exercised against
//    IPersistentState - the WAL is the actual source of truth.
//
// 2. The Azure-Tables WAL provider (Orleans.Lattice.Storage.AzureTable)
//    still gets the full append path; this null storage only replaces
//    the *leaf/internal-grain checkpoint* that runs after the WAL ack
//    inside `BPlusLeafGrain.CommitSetManyAsync` /
//    `BPlusInternalGrain.WriteStateAsync`. The WAL replay path on a
//    real restart would still rebuild leaf/internal state from the
//    persisted log, which is exactly what production durability
//    relies on.
//
// 3. The benchmark is short-lived (~120 s) and runs in a single silo
//    container that is terminated at the end of the rung. No
//    cross-restart semantic is observed.
//
// In other words, NullGrainStorage is an A/B knob that removes the
// `memorystorage/N` activation chokepoint so we can measure the WAL
// pipeline's true ceiling. It is **not** appropriate for any
// production deployment and is registered only when
// `BENCH_LEAF_STORAGE_KIND=null` is set.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Storage;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

internal sealed class NullGrainStorage : IGrainStorage
{
    public Task ReadStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
        => Task.CompletedTask;

    public Task WriteStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
        => Task.CompletedTask;

    public Task ClearStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
        => Task.CompletedTask;
}

internal static class NullGrainStorageExtensions
{
    public static ISiloBuilder AddNullGrainStorageAsDefault(this ISiloBuilder builder)
        => builder.AddNullGrainStorage(Orleans.Providers.ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);

    public static ISiloBuilder AddNullGrainStorage(this ISiloBuilder builder, string name)
        => builder.ConfigureServices(services =>
        {
            services.AddGrainStorage(name, (sp, providerName) => new NullGrainStorage());
        });
}
