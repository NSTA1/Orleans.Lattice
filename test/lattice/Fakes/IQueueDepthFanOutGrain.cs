namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Driver grain that fans concurrent calls out to an
/// <see cref="IQueueDepthProbeGrain"/> from <em>inside</em> a silo.
/// <para>
/// The origin matters. The grain-call observation filter is registered on the
/// silo, so it observes the silo's outgoing calls; a call issued by the test's
/// external cluster client leaves from the client and is not seen. Driving the
/// fan-out from a grain therefore exercises the same seam that carries real
/// grain-to-grain traffic - a shard calling its leaves, for instance - rather
/// than a seam that only the test would ever use.
/// </para>
/// </summary>
public interface IQueueDepthFanOutGrain : IGrainWithStringKey
{
    /// <summary>
    /// Dispatches <paramref name="count"/> concurrent calls to the probe grain
    /// identified by <paramref name="probeKey"/> and waits for all of them.
    /// </summary>
    /// <param name="probeKey">The probe grain's primary key.</param>
    /// <param name="count">How many concurrent calls to dispatch.</param>
    Task FanOutAsync(string probeKey, int count);
}
