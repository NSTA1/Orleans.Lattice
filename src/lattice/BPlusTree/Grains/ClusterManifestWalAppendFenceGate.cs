using Microsoft.Extensions.Logging;
using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalAppendFenceGate"/> (epic #5605): open while every
/// <see cref="SiloStatus.Active"/> member of the current membership snapshot has
/// a grain manifest that advertises <see cref="IWalAppendFenceCapable"/>. A
/// member whose manifest has not reached this silo yet counts as not capable, so
/// the gate fails closed during a rolling upgrade, opens by itself once the last
/// old-build silo has left, and closes again if one joins. The verdict is cached
/// against the manifest and membership versions, so a leaf can consult it on
/// every append. Same shape as <see cref="ClusterManifestWalClockFloorGate"/>,
/// whose verdict function it shares.
/// </summary>
internal sealed class ClusterManifestWalAppendFenceGate : IWalAppendFenceGate
{
    private readonly IClusterManifestProvider _manifests;
    private readonly IClusterMembershipService _membership;
    private readonly ILogger<ClusterManifestWalAppendFenceGate> _logger;
    private readonly GrainInterfaceType _marker;
    private readonly object _gate = new();
    private MajorMinorVersion _manifestVersion = MajorMinorVersion.MinValue;
    private MembershipVersion _membershipVersion = MembershipVersion.MinValue;
    private bool _open;
    private bool _evaluated;

    /// <summary>Initialises the gate over the silo's manifest and membership views.</summary>
    public ClusterManifestWalAppendFenceGate(
        IClusterManifestProvider manifests,
        IClusterMembershipService membership,
        GrainInterfaceTypeResolver interfaceTypes,
        ILogger<ClusterManifestWalAppendFenceGate> logger)
    {
        ArgumentNullException.ThrowIfNull(manifests);
        ArgumentNullException.ThrowIfNull(membership);
        ArgumentNullException.ThrowIfNull(interfaceTypes);
        ArgumentNullException.ThrowIfNull(logger);
        _manifests = manifests;
        _membership = membership;
        _logger = logger;
        _marker = interfaceTypes.GetGrainInterfaceType(typeof(IWalAppendFenceCapable));
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
                if (_evaluated && _open)
                {
                    return true; // RED-PROOF: gate ratcheted
                }

                if (_evaluated && manifest.Version == _manifestVersion && snapshot.Version == _membershipVersion)
                {
                    return _open;
                }

                var open = ClusterManifestWalClockFloorGate.Evaluate(manifest, snapshot, _marker);
                if (!_evaluated || open != _open)
                {
                    if (open)
                    {
                        _logger.LogInformation("WAL append fence gate is open: every active silo is fence-capable");
                    }
                    else
                    {
                        _logger.LogWarning(
                            "WAL append fence gate is closed: not every active silo advertises {Marker}; leaves append unstamped until it does",
                            nameof(IWalAppendFenceCapable));
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
}
