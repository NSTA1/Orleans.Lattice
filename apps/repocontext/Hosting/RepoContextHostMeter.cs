namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The single name of the meter every host-level component publishes its
/// instruments on.
/// </summary>
/// <remarks>
/// <para>
/// This exists so that no host component has to borrow another component's meter
/// name constant. Three of them previously referenced
/// <see cref="RepoContextDrainForecastService"/>'s, which made an unrelated
/// service the de facto owner of the drain forecast's, the garbage collector's
/// and the backup surface's series. Renaming that constant for a reason having
/// nothing to do with any of them would have silently relocated all three.
/// </para>
/// <para>
/// <b>Only a departure from the prefix is harmful, and that is what the test
/// pins.</b> Prometheus series here are named from the <em>instrument</em>, not
/// from the meter, so renaming this constant to anything still under
/// <see cref="RepoContextMetricsCollector.MeterNamePrefix"/> changes nothing an
/// operator can see. Moving it out from under that prefix, however, ends the
/// collector's subscription and every series on it disappears from
/// <c>/metrics</c> at once - with no error, and with the exposition still
/// looking complete. That is precisely the silent-absence failure the instrument
/// descriptions warn readers about, so it is asserted rather than assumed.
/// </para>
/// </remarks>
public static class RepoContextHostMeter
{
    /// <summary>
    /// The meter name. Must remain under
    /// <see cref="RepoContextMetricsCollector.MeterNamePrefix"/>.
    /// </summary>
    public const string Name = "orleans.lattice.repocontext.host";
}
