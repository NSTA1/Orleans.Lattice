using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for resolving cluster-internal
/// <see cref="ILatticeQueue{T}"/> instances from an
/// <see cref="IGrainFactory"/>.
/// </summary>
public static class LatticeQueueExtensions
{
    /// <summary>
    /// Resolves the cluster-internal FIFO queue named
    /// <paramref name="queueName"/>, serializing values with
    /// <paramref name="serializer"/> (defaulting to
    /// <see cref="JsonLatticeSerializer{T}.Default"/> when
    /// <see langword="null"/>). Each distinct <paramref name="queueName"/>
    /// is an independent queue backed by its own reserved system tree;
    /// resolving the same name returns a facade over the same coordinator
    /// grain.
    /// </summary>
    /// <typeparam name="T">The queued value type.</typeparam>
    /// <param name="grainFactory">The grain factory to resolve the queue from.</param>
    /// <param name="queueName">The logical queue name (also the coordinator grain key).</param>
    /// <param name="serializer">
    /// Optional value serializer. Defaults to
    /// <see cref="JsonLatticeSerializer{T}.Default"/>.
    /// </param>
    /// <returns>A typed facade over the named queue.</returns>
    public static ILatticeQueue<T> GetLatticeQueue<T>(
        this IGrainFactory grainFactory,
        string queueName,
        ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(queueName);

        var grain = grainFactory.GetGrain<ILatticeQueueGrain>(queueName);
        return new LatticeQueue<T>(grain, serializer ?? JsonLatticeSerializer<T>.Default);
    }
}
