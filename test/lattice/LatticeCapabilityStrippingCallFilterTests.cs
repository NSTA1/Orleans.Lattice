using System.Net;
using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Security-regression coverage for <see cref="LatticeCapabilityStrippingCallFilter"/>'s
/// trust decision. The in-silo hosted-client exemption (which retains the reserved
/// internal capability keys - system-origin, view scopes, internal-origin,
/// maintenance, apply-offset - instead of stripping them) must be derived from the
/// <see cref="SiloAddress"/> embedded in the <c>hosted-{addr}</c> client id
/// validated against live cluster membership, NOT from the client-suppliable
/// <c>hosted-</c> prefix alone. An external Orleans client fully controls its
/// announced client id, so a bare-prefix check let it announce a <c>hosted-*</c> id,
/// keep a forged <c>ol.sysorig</c> marker, and fully bypass the access gate. These
/// tests prove a forged <c>hosted-*</c> id is treated as external (keys stripped)
/// while the genuine local and active cross-silo hosted clients remain trusted.
/// </summary>
[TestFixture]
public sealed class LatticeCapabilityStrippingCallFilterTests
{
    private static readonly SiloAddress LocalSilo =
        SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);
    private static readonly SiloAddress ActiveRemoteSilo =
        SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11112), 2);
    private static readonly SiloAddress DeadSilo =
        SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11113), 3);

    private static readonly string[] ReservedKeys =
    [
        LatticeEventConstants.AccessGateSystemOriginRequestContextKey,
        LatticeEventConstants.ViewWriteRequestContextKey,
        LatticeEventConstants.ViewReadRequestContextKey,
        LatticeEventConstants.InternalGrainOriginRequestContextKey,
        LatticeEventConstants.MaintenanceRequestContextKey,
        LatticeEventConstants.CommitLogSourceRequestContextKey,
        LatticeEventConstants.ApplyOffsetRequestContextKey,
        LatticeEventConstants.ApplyOffsetPartitionRequestContextKey,
    ];

    [TearDown]
    public void TearDown() => RequestContext.Clear();

    private static ISiloStatusOracle Oracle()
    {
        var oracle = Substitute.For<ISiloStatusOracle>();
        oracle.SiloAddress.Returns(LocalSilo);
        oracle.GetApproximateSiloStatus(Arg.Any<SiloAddress>()).Returns(SiloStatus.None);
        oracle.GetApproximateSiloStatus(LocalSilo).Returns(SiloStatus.Active);
        oracle.GetApproximateSiloStatus(ActiveRemoteSilo).Returns(SiloStatus.Active);
        oracle.GetApproximateSiloStatus(DeadSilo).Returns(SiloStatus.Dead);
        return oracle;
    }

    private static IIncomingGrainCallContext ContextWithSource(GrainId sourceId)
    {
        var context = Substitute.For<IIncomingGrainCallContext>();
        context.SourceId.Returns(sourceId);
        context.Invoke().Returns(Task.CompletedTask);
        return context;
    }

    // Mirrors Orleans' own hosted-client id shape (grain type == the client type,
    // key == "hosted-{siloAddress}") without depending on the internal ClientGrainId
    // factory, so IsClient() is true and the filter parses the embedded address.
    private static GrainId HostedClientId(SiloAddress addr) =>
        GrainId.Create(GrainTypePrefix.ClientGrainType, "hosted-" + addr.ToParsableString());

    // Runs the filter against a client-sourced call that has seeded every reserved
    // capability key, and returns how many of those keys survived. A trusted source
    // keeps all of them (the filter stamps internal-origin, strips nothing); an
    // untrusted (external / forged) source has every one stripped.
    private static async Task<int> SurvivingReservedKeysAsync(GrainId sourceId)
    {
        var filter = new LatticeCapabilityStrippingCallFilter(Oracle());
        foreach (var key in ReservedKeys)
        {
            RequestContext.Set(key, true);
        }

        await filter.Invoke(ContextWithSource(sourceId));

        return ReservedKeys.Count(k => RequestContext.Get(k) is not null);
    }

    [Test]
    public async Task Local_in_silo_hosted_client_retains_all_capability_keys()
    {
        var surviving = await SurvivingReservedKeysAsync(HostedClientId(LocalSilo));
        Assert.That(surviving, Is.EqualTo(ReservedKeys.Length));
    }

    [Test]
    public async Task Active_cross_silo_hosted_client_retains_all_capability_keys()
    {
        var surviving = await SurvivingReservedKeysAsync(HostedClientId(ActiveRemoteSilo));
        Assert.That(surviving, Is.EqualTo(ReservedKeys.Length));
    }

    [Test]
    public async Task Forged_hosted_prefix_from_external_client_has_all_capability_keys_stripped()
    {
        // A client-controlled id whose "hosted-" suffix is not a parseable silo
        // address. Before the fix the bare-prefix check trusted it, leaving the
        // forged ol.sysorig marker in place for a full access-gate bypass.
        var forged = GrainId.Create(GrainTypePrefix.ClientGrainType, "hosted-forged");
        var surviving = await SurvivingReservedKeysAsync(forged);
        Assert.That(surviving, Is.EqualTo(0));
    }

    [Test]
    public async Task Hosted_id_naming_a_dead_silo_has_all_capability_keys_stripped()
    {
        // A well-formed "hosted-{addr}" id whose address is not a live cluster
        // member: an external client that guessed / replayed a stale silo address.
        var surviving = await SurvivingReservedKeysAsync(HostedClientId(DeadSilo));
        Assert.That(surviving, Is.EqualTo(0));
    }

    [Test]
    public async Task Ordinary_external_client_has_all_capability_keys_stripped()
    {
        var external = GrainId.Create(GrainTypePrefix.ClientGrainType, Guid.NewGuid().ToString("N"));
        var surviving = await SurvivingReservedKeysAsync(external);
        Assert.That(surviving, Is.EqualTo(0));
    }
}
