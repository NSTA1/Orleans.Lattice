using System.Collections;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization.Configuration;
using Orleans.Serialization.Invocation;
using Orleans.Serialization.TypeSystem;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the "method unknown" failure of the append fence's gate race (epic
/// #5605, design note section 4, "Racing the gate"): a silo whose build predates
/// the fence - here, a silo whose type manifest has had the new
/// <see cref="IWalShardGrain"/> invokables removed, which is exactly what a
/// 10.2.0 manifest lacks - receives a stamped method before the caller's gate has
/// observed it join. The test records the failure's exact shape and proves
/// <see cref="WalAppendFenceMethodProbe"/> recognises it, so the caller re-issues
/// the append unstamped only on that definite failure, while the existing methods
/// keep working on the older silo.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalAppendFenceMixedVersionTests
{
    /// <summary>The <see cref="IWalShardGrain"/> methods a 10.2.0 build does not have.</summary>
    private static readonly string[] NewMethods =
    {
        nameof(IWalShardGrain.AppendStampedAsync),
        nameof(IWalShardGrain.AppendBatchStampedAsync),
        nameof(IWalShardGrain.ClaimAppendEpochAsync),
        nameof(IWalShardGrain.ClaimRoutingGenerationAsync),
        nameof(IWalShardGrain.RetireAppendEpochAsync),
    };

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<OlderBuildSiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static WalRecord Entry(string treeId, string key) => new()
    {
        TreeId = treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    [Test]
    public void The_older_build_manifest_lacks_every_new_invokable()
    {
        Assert.That(InvokablesFor(NewMethods), Has.Count.EqualTo(NewMethods.Length),
            "precondition: each new method has exactly one generated invokable to remove");
    }

    [Test]
    public async Task A_stamped_append_to_an_older_build_fails_definitely_as_method_unknown()
    {
        var shard = _cluster.Client.GetGrain<IWalShardGrain>("mixed-version/0");

        Exception? failure = null;
        try
        {
            await shard.AppendBatchStampedAsync(
                new[] { Entry("mixed-version", "k") }, WalAppendStamp.ForLeaf(Guid.NewGuid(), 1), CancellationToken.None);
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        Assert.That(failure, Is.Not.Null, "an older build cannot serve a method it does not have");
        Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(failure!), Is.True,
            $"the failure shape must be recognised as definite; it was {failure!.GetType().FullName}: {failure.Message} "
            + $"(inner: {failure.InnerException?.GetType().FullName}: {failure.InnerException?.Message})");
    }

    [Test]
    public void A_claim_to_an_older_build_fails_definitely_as_method_unknown()
    {
        var shard = _cluster.Client.GetGrain<IWalShardGrain>("mixed-version/1");

        var failure = Assert.CatchAsync(() => shard.ClaimAppendEpochAsync(Guid.NewGuid(), 1, CancellationToken.None));

        Assert.That(WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(failure!), Is.True,
            $"{failure!.GetType().FullName}: {failure.Message}");
    }

    [Test]
    public async Task The_existing_unstamped_methods_keep_working_on_the_older_build()
    {
        var shard = _cluster.Client.GetGrain<IWalShardGrain>("mixed-version/2");

        var offsets = await shard.AppendBatchAsync(new[] { Entry("mixed-version", "k") }, CancellationToken.None);

        Assert.That(offsets, Has.Count.EqualTo(1), "an unstamped append from a newer silo is served as 10.2.0 serves it");
    }

    /// <summary>The generated invokable types of the named <see cref="IWalShardGrain"/> methods.</summary>
    private static List<Type> InvokablesFor(IReadOnlyCollection<string> methods)
    {
        var found = new List<Type>();
        foreach (var type in typeof(IWalShardGrain).Assembly.GetTypes())
        {
            if (type.IsAbstract || !typeof(IInvokable).IsAssignableFrom(type) || type.GetConstructor(Type.EmptyTypes) is null)
            {
                continue;
            }

            if (!type.Name.Contains(nameof(IWalShardGrain), StringComparison.Ordinal))
            {
                continue;
            }

            var invokable = (IInvokable)Activator.CreateInstance(type)!;
            if (methods.Contains(invokable.GetMethodName()) && invokable.GetInterfaceName().EndsWith(nameof(IWalShardGrain), StringComparison.Ordinal))
            {
                found.Add(type);
            }
        }

        return found;
    }

    /// <summary>Removes every node of <paramref name="tree"/> whose value is one of <paramref name="types"/>.</summary>
    private static void Strip(CompoundTypeAliasTree tree, IReadOnlySet<Type> types)
    {
        var childrenField = typeof(CompoundTypeAliasTree).GetField("_children", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("CompoundTypeAliasTree no longer keeps its children in _children.");
        if (childrenField.GetValue(tree) is not IDictionary children)
        {
            return;
        }

        var doomed = new List<object>();
        foreach (DictionaryEntry child in children)
        {
            var node = (CompoundTypeAliasTree)child.Value!;
            if (node.Value is { } value && types.Contains(value))
            {
                doomed.Add(child.Key);
            }
            else
            {
                Strip(node, types);
            }
        }

        foreach (var key in doomed)
        {
            children.Remove(key);
        }
    }

    /// <summary>
    /// Removes <paramref name="types"/>, and every generated codec, activator and
    /// copier whose target is one of them, from each type collection of the
    /// manifest. Orleans formats every such target when the silo starts, so an
    /// alias removed without its helpers fails the silo's start rather than the
    /// call; with both gone, the silo's manifest is a 10.2.0 manifest.
    /// </summary>
    private static void StripMetadata(TypeManifestOptions options, IReadOnlySet<Type> types)
    {
        bool Targets(Type candidate) =>
            types.Contains(candidate)
            || candidate.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericArguments().Any(types.Contains));

        var removed = 0;
        foreach (var property in typeof(TypeManifestOptions).GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (property.GetIndexParameters().Length != 0)
            {
                continue;
            }

            switch (property.GetValue(options))
            {
                case HashSet<Type> set:
                    removed += set.RemoveWhere(Targets);
                    break;
                case IDictionary dictionary when property.PropertyType.IsGenericType
                    && property.PropertyType.GetGenericArguments()[0] == typeof(Type):
                    foreach (var key in dictionary.Keys.Cast<Type>().Where(Targets).ToList())
                    {
                        dictionary.Remove(key);
                        removed++;
                    }

                    break;
            }
        }

        if (removed == 0)
        {
            throw new InvalidOperationException("No generated helper of the new invokables was found in the type manifest.");
        }
    }

    private sealed class OlderBuildSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            var types = InvokablesFor(NewMethods).ToHashSet();
            siloBuilder.Services.PostConfigure<TypeManifestOptions>(options =>
            {
                StripMetadata(options, types);
                Strip(options.CompoundTypeAliases, types);
            });
        }
    }
}
