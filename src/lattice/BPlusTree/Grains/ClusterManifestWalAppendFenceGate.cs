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
/// <remarks>
/// Every leaf append on the silo reads <see cref="IsOpen"/>, so the cached verdict
/// is published as one immutable snapshot through a volatile field and read without
/// a lock (#5605 review, S3): an append compares two versions against it. The lock
/// is taken only when a version has moved, to re-evaluate and log a transition.
/// </remarks>
internal sealed class ClusterManifestWalAppendFenceGate : IWalAppendFenceGate
{
    private readonly IClusterManifestProvider _manifests;
    private readonly IClusterMembershipService _membership;
    private readonly ILogger<ClusterManifestWalAppendFenceGate> _logger;
    private readonly GrainInterfaceType _marker;
    private readonly object _gate = new();
    private volatile Verdict? _verdict;

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
            var cached = _verdict;
            if (cached is not null && cached.Matches(manifest.Version, snapshot.Version))
            {
                return cached.Open;
            }

            lock (_gate)
            {
                var previous = _verdict;
                if (previous is not null && previous.Matches(manifest.Version, snapshot.Version))
                {
                    return previous.Open;
                }

                var open = ClusterManifestWalClockFloorGate.Evaluate(manifest, snapshot, _marker);
                if (previous is null || open != previous.Open)
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

                _verdict = new Verdict(manifest.Version, snapshot.Version, open);
                return open;
            }
        }
    }

    /// <summary>The verdict for one pair of manifest and membership versions; immutable.</summary>
    private sealed class Verdict(MajorMinorVersion manifestVersion, MembershipVersion membershipVersion, bool open)
    {
        public bool Open { get; } = open;

        public bool Matches(MajorMinorVersion manifest, MembershipVersion membership) =>
            manifest == manifestVersion && membership == membershipVersion;
    }
}
