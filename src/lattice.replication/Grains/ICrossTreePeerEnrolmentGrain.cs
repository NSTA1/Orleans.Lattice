using Orleans.Concurrency;
using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The durable set of peers that have ever attached a shipper to a tree
/// (issue #4684), keyed by the logical tree name. The cross-tree decision
/// purge hold waits for every one of them as well as every configured peer:
/// removing a peer from the topology only detaches it, and a detached peer
/// that is added back resumes against barriers that still need the decision.
/// </summary>
[Alias(ReplicationTypeAliases.ICrossTreePeerEnrolmentGrain)]
internal interface ICrossTreePeerEnrolmentGrain : IGrainWithStringKey
{
    /// <summary>Every enrolled peer.</summary>
    [AlwaysInterleave]
    Task<ImmutableArray<string>> GetAsync();

    /// <summary>Durably enrols <paramref name="peerClusterId"/>. Idempotent.</summary>
    Task EnrolAsync(string peerClusterId);
}
