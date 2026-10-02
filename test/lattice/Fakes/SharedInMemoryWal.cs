using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// One in-memory WAL store per test cluster, shared by every in-process silo of
/// that cluster (issue #4196).
/// <para>
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> registers
/// <see cref="InMemoryWalStorageProvider"/> as a per-container singleton, so in a
/// multi-silo in-process <see cref="TestCluster"/> each silo gets its own WAL. A WAL
/// shard that activates on a different silo from the one that wrote - or any
/// per-silo reader such as the WAL GC - then reads a different log, and an
/// assertion about WAL state describes the harness rather than the product. A
/// durable provider is one store the whole cluster sees; this helper restores that.
/// </para>
/// <para>
/// The store is keyed by the cluster's <see cref="ClusterOptions.ServiceId"/> and
/// <see cref="ClusterOptions.ClusterId"/>, so two clusters in one process (a
/// replication pair, or consecutive fixtures) never share a WAL. It lives while any
/// silo of its cluster is running and is dropped when the last one is disposed, so a
/// cluster that is stopped entirely and redeployed starts from an empty WAL; a
/// fixture that must keep its WAL across a whole-cluster restart keeps its own static
/// provider instead.
/// </para>
/// </summary>
internal static class SharedInMemoryWal
{
    private static readonly ConcurrentDictionary<string, Store> Stores = new(StringComparer.Ordinal);
    private static readonly object Gate = new();

    /// <summary>Whether a running silo still holds the WAL store of the cluster <paramref name="options"/> names.</summary>
    internal static bool IsLive(ClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Stores.ContainsKey(KeyFor(options));
    }

    /// <summary>
    /// Adds a silo configurator that backs every silo of the built cluster with the
    /// cluster's shared WAL store. The registration replaces the in-memory default
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> installs, whatever
    /// the configurator order.
    /// </summary>
    public static TestClusterBuilder UseSharedInMemoryWal(this TestClusterBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        return builder;
    }

    /// <summary>
    /// Backs this silo's baseline <see cref="IWalStorageProvider"/> with the shared
    /// store of the silo's cluster.
    /// </summary>
    public static ISiloBuilder AddSharedInMemoryWalStorage(this ISiloBuilder siloBuilder)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.Services.AddSingleton<Lease>();

        // Taking the lease at silo start, not on first WAL use, keeps the store alive
        // for a silo that has not touched its WAL yet when a sibling silo is killed.
        siloBuilder.Services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(
            sp => new EagerLease(sp.GetRequiredService<Lease>()));
        siloBuilder.AddWalStorage(sp => sp.GetRequiredService<Lease>().Provider);
        return siloBuilder;
    }

    /// <summary>
    /// Fails unless every silo of <paramref name="cluster"/> observes the same WAL:
    /// an entry appended through the first silo's baseline provider must be read
    /// back through every other silo's. Call it after deploying a multi-silo fixture
    /// and before any test asserts on WAL state. The probe entry is written to a
    /// throwaway tree id and trimmed afterwards.
    /// </summary>
    /// <exception cref="InvalidOperationException">A silo is not in-process, or reads a different WAL.</exception>
    public static async Task AssertAllSilosShareOneWalAsync(TestCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        var silos = cluster.Silos.ToArray();
        if (silos.Length < 2)
        {
            return;
        }

        var providers = new IWalStorageProvider[silos.Length];
        for (var i = 0; i < silos.Length; i++)
        {
            if (silos[i] is not InProcessSiloHandle inProcess)
            {
                throw new InvalidOperationException(
                    $"Silo {silos[i].SiloAddress} is not an in-process silo, so its WAL cannot be inspected.");
            }

            providers[i] = inProcess.SiloHost.Services.GetRequiredService<IWalStorageProvider>();
        }

        var probeTree = $"__harness-wal-probe-{Guid.NewGuid():N}";
        var probe = new WalEntry
        {
            Offset = 0,
            Mutation = new LatticeMutation { TreeId = probeTree, Kind = MutationKind.Set, Key = "probe", Value = [1] },
        };

        await providers[0].AppendBatchAsync(probeTree, 0, [probe], CancellationToken.None);
        try
        {
            for (var i = 1; i < providers.Length; i++)
            {
                var seen = false;
                await foreach (var entry in providers[i].ReadAsync(probeTree, 0, -1L, 1, CancellationToken.None))
                {
                    seen = entry.Offset == 0 && entry.Mutation.Key == "probe";
                }

                if (!seen)
                {
                    throw new InvalidOperationException(
                        $"Silo {silos[i].SiloAddress} does not observe the WAL written through silo "
                        + $"{silos[0].SiloAddress}: each silo has its own WAL store, so WAL state asserted by a "
                        + "test would describe the harness rather than the product (issue #4196). Configure the "
                        + "cluster with UseSharedInMemoryWal() or a provider instance every silo shares.");
                }
            }
        }
        finally
        {
            await providers[0].TrimAsync(probeTree, 0, 0, CancellationToken.None);
        }
    }

    private static string KeyFor(ClusterOptions options) => $"{options.ServiceId}/{options.ClusterId}";

    private sealed class Store
    {
        public InMemoryWalStorageProvider Provider { get; } = new();

        public int Holders { get; set; }
    }

    /// <summary>One silo's hold on its cluster's store, released when the silo's container is disposed.</summary>
    internal sealed class Lease : IDisposable
    {
        private readonly string _key;
        private int _disposed;

        public Lease(IOptions<ClusterOptions> options)
        {
            _key = KeyFor(options.Value);
            lock (Gate)
            {
                var store = Stores.GetOrAdd(_key, static _ => new Store());
                store.Holders++;
                Provider = store.Provider;
            }
        }

        public InMemoryWalStorageProvider Provider { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            lock (Gate)
            {
                if (Stores.TryGetValue(_key, out var store) && --store.Holders == 0)
                {
                    Stores.TryRemove(_key, out _);
                }
            }
        }
    }

    private sealed class EagerLease(Lease lease) : ILifecycleParticipant<ISiloLifecycle>
    {
        public void Participate(ISiloLifecycle lifecycle) => GC.KeepAlive(lease);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => siloBuilder.AddSharedInMemoryWalStorage();
    }
}
