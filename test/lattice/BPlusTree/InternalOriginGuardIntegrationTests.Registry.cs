using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed partial class InternalOriginGuardIntegrationTests
{
    [TestCase("register")]
    [TestCase("register-and-get")]
    [TestCase("update")]
    public async Task Registry_routing_writes_from_external_clients_are_refused_without_mutation(string verb)
    {
        var logical = "registry-guard-" + verb;
        var internalFactory = _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IGrainFactory>();
        var trusted = internalFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var original = new TreeRegistryEntry { ShardCount = 1 };
        if (verb == "update")
        {
            await trusted.RegisterAsync(logical, original);
        }

        var external = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var forged = new TreeRegistryEntry
        {
            PhysicalTreeId = "t/victim/secrets",
            ShardCount = 1,
        };
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);
        RequestContext.Set(LatticeEventConstants.InternalGrainOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
        {
            switch (verb)
            {
                case "register":
                    await external.RegisterAsync(logical, forged);
                    break;
                case "register-and-get":
                    await external.RegisterAndGetEntryAsync(logical, forged);
                    break;
                default:
                    await external.UpdateAsync(logical, forged);
                    break;
            }
        });

        RequestContext.Clear();
        var persisted = await trusted.GetEntryAsync(logical);
        Assert.That(persisted?.PhysicalTreeId, Is.Null);
        Assert.That(persisted is not null, Is.EqualTo(verb == "update"));
    }

    [Test]
    public async Task Registry_internal_registration_update_and_read_remain_available()
    {
        var internalFactory = _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IGrainFactory>();
        var registry = internalFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        const string logical = "registry-guard-trusted";
        var (created, entry) = await registry.RegisterAndGetEntryAsync(logical);
        Assert.That(created, Is.True);
        Assert.That(entry, Is.Not.Null);

        await registry.UpdateAsync(logical, entry! with { MaxLeafKeys = 64 });
        Assert.That((await registry.GetEntryAsync(logical))!.MaxLeafKeys, Is.EqualTo(64));
    }
}
