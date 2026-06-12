using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Configures the WAL-saturation receiver flow-control mapping and ensures
    /// <see cref="WalSaturationReceiverFlowControlPolicy"/> is the active
    /// <see cref="IReceiverFlowControlPolicy"/>. The policy is already the
    /// default installed by
    /// <see cref="AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions})"/>;
    /// call this method when you want to tune the throttled / saturated
    /// batch-size and pause mapping, or to force the policy back on after a
    /// host has replaced it (for example, after pre-registering
    /// <see cref="NoOpReceiverFlowControlPolicy"/>).
    /// <para>
    /// Safe to call before or after
    /// <see cref="AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions})"/>:
    /// this method removes any prior <see cref="IReceiverFlowControlPolicy"/>
    /// registration and installs the saturation policy, so the result is
    /// deterministic regardless of composition order. A host that wants a
    /// different policy simply registers its own
    /// <see cref="IReceiverFlowControlPolicy"/> after this call.
    /// </para>
    /// <para>
    /// The policy degrades to <see cref="ReceiverFlowControlHint.None"/> when
    /// no <see cref="IWalSaturationSignal"/> is registered (the signal is
    /// produced by <c>AddLattice</c>), so a misconfigured host keeps the
    /// existing blind-push behaviour rather than failing.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate to tune the throttled /
    /// saturated batch-size and pause mapping. Applied to every named
    /// (per-tree) <see cref="WalSaturationReceiverFlowControlOptions"/>
    /// instance as the cluster-wide baseline.</param>
    public static Orleans.Hosting.ISiloBuilder AddWalSaturationReceiverFlowControl(
        this Orleans.Hosting.ISiloBuilder builder,
        Action<WalSaturationReceiverFlowControlOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddOptions<WalSaturationReceiverFlowControlOptions>();
        if (configure is not null)
        {
            builder.Services.ConfigureAll(configure);
        }

        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            if (builder.Services[i].ServiceType == typeof(IReceiverFlowControlPolicy))
            {
                builder.Services.RemoveAt(i);
            }
        }

        builder.Services.AddSingleton<IReceiverFlowControlPolicy>(sp =>
            new WalSaturationReceiverFlowControlPolicy(
                sp.GetService<IWalSaturationSignal>(),
                sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>()));

        return builder;
    }
}
