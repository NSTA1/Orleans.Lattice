using Microsoft.Extensions.Logging;
using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Lattice.Views;

/// <summary>
/// Automatically admits joint-repair work once the cluster membership and
/// manifests establish that no remaining silo runs the old protocol. Joining
/// and draining members are included: their old activations cannot be assumed
/// gone merely because those members are not active placement candidates.
/// </summary>
internal sealed class ClusterManifestViewJointRepairAdmissionGate(
    IClusterManifestProvider manifests,
    IClusterMembershipService membership,
    GrainInterfaceTypeResolver interfaceTypes,
    ILogger<ClusterManifestViewJointRepairAdmissionGate> logger) : IViewJointRepairAdmissionGate
{
    private readonly GrainInterfaceType _marker = interfaceTypes.GetGrainInterfaceType(typeof(IViewJointRepairCapable));
    private readonly object _gate = new();
    private MajorMinorVersion _manifestVersion = MajorMinorVersion.MinValue;
    private MembershipVersion _membershipVersion = MembershipVersion.MinValue;
    private bool _open;
    private bool _evaluated;

    /// <inheritdoc />
    public Task<bool> IsOpenAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var manifest = manifests.Current;
        var snapshot = membership.CurrentSnapshot;
        lock (_gate)
        {
            if (_evaluated && manifest.Version == _manifestVersion && snapshot.Version == _membershipVersion)
            {
                return Task.FromResult(_open);
            }

            var open = Evaluate(manifest, snapshot, _marker);
            if (!_evaluated || open != _open)
            {
                if (open)
                {
                    logger.LogInformation(
                        "Joint-view repair admission is open: every remaining silo advertises repair support.");
                }
                else
                {
                    logger.LogWarning(
                        "Joint-view repair admission is paused: a remaining silo has missing or legacy repair capability; existing repair ownership remains enforced.");
                }
            }

            _manifestVersion = manifest.Version;
            _membershipVersion = snapshot.Version;
            _open = open;
            _evaluated = true;
            return Task.FromResult(open);
        }
    }

    internal static bool Evaluate(
        ClusterManifest manifest,
        ClusterMembershipSnapshot snapshot,
        GrainInterfaceType marker)
    {
        var hasActive = false;
        foreach (var member in snapshot.Members.Values)
        {
            if (member.Status == SiloStatus.Dead)
            {
                continue;
            }

            hasActive |= member.Status == SiloStatus.Active;
            if (!manifest.Silos.TryGetValue(member.SiloAddress, out var siloManifest)
                || !siloManifest.Interfaces.ContainsKey(marker))
            {
                return false;
            }
        }

        return hasActive;
    }
}
