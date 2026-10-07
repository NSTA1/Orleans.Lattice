using Microsoft.Extensions.Logging;
using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalClockFloorGate"/>: open while every
/// <see cref="SiloStatus.Active"/> member of the current membership snapshot has
/// a grain manifest that advertises <see cref="IWalClockFloorCapable"/>. A
/// member whose manifest has not reached this silo yet counts as not capable, so
/// the gate fails closed during a rolling upgrade and opens by itself once the
/// last old-build silo has left. The verdict is cached against the manifest and
/// membership versions, so a WAL partition can consult it on every shipping read.
/// </summary>
internal sealed class ClusterManifestWalClockFloorGate : IWalClockFloorGate
{
    private readonly IClusterManifestProvider _manifests;
    private readonly IClusterMembershipService _membership;
    private readonly ILogger<ClusterManifestWalClockFloorGate> _logger;
    private readonly GrainInterfaceType _marker;
    private readonly object _gate = new();
    private MajorMinorVersion _manifestVersion = MajorMinorVersion.MinValue;
    private MembershipVersion _membershipVersion = MembershipVersion.MinValue;
    private bool _open;
    private bool _evaluated;

    /// <summary>Initialises the gate over the silo's manifest and membership views.</summary>
    public ClusterManifestWalClockFloorGate(
        IClusterManifestProvider manifests,
        IClusterMembershipService membership,
        GrainInterfaceTypeResolver interfaceTypes,
        ILogger<ClusterManifestWalClockFloorGate> logger)
    {
        ArgumentNullException.ThrowIfNull(manifests);
        ArgumentNullException.ThrowIfNull(membership);
        ArgumentNullException.ThrowIfNull(interfaceTypes);
        ArgumentNullException.ThrowIfNull(logger);
        _manifests = manifests;
        _membership = membership;
        _logger = logger;
        _marker = interfaceTypes.GetGrainInterfaceType(typeof(IWalClockFloorCapable));
    }

    /// <inheritdoc />
    public bool IsOpen
    {
        get
        {
            var manifest = _manifests.Current;
            var snapshot = _membership.CurrentSnapshot;
            lock (_gate)
            {
                if (_evaluated && manifest.Version == _manifestVersion && snapshot.Version == _membershipVersion)
                {
                    return _open;
                }

                var open = Evaluate(manifest, snapshot, _marker);
                if (!_evaluated || open != _open)
                {
                    if (open)
                    {
                        _logger.LogInformation("WAL clock floor gate is open: every active silo is floor-capable");
                    }
                    else
                    {
                        _logger.LogWarning(
                            "WAL clock floor gate is closed: not every active silo advertises {Marker}; WAL clock floors stop advancing until it does",
                            nameof(IWalClockFloorCapable));
                    }
                }

                _manifestVersion = manifest.Version;
                _membershipVersion = snapshot.Version;
                _open = open;
                _evaluated = true;
                return open;
            }
        }
    }

    /// <summary>
    /// The verdict over one manifest and membership view: every active member has
    /// a manifest advertising <paramref name="marker"/>, and there is at least one.
    /// </summary>
    internal static bool Evaluate(ClusterManifest manifest, ClusterMembershipSnapshot snapshot, GrainInterfaceType marker)
    {
        var active = 0;
        foreach (var member in snapshot.Members.Values)
        {
            if (member.Status != SiloStatus.Active)
            {
                continue;
            }

            active++;
            if (!manifest.Silos.TryGetValue(member.SiloAddress, out var siloManifest)
                || !siloManifest.Interfaces.ContainsKey(marker))
            {
                return false;
            }
        }

        return active > 0;
    }
}
