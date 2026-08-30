using System.Collections.Immutable;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Constructor guard tests and shared helpers for state API edge-case partials.
/// </summary>
[TestFixture]
public sealed partial class LatticeStateApiEdgeCaseTests
{
    private static readonly LatticeSubject NamedSubject = new("alice");

    [TestCase("grainFactory")]
    [TestCase("options")]
    [TestCase("apiOptions")]
    [TestCase("services")]
    [TestCase("tenantResolver")]
    public void LatticeStateQuery_constructor_rejects_a_null_dependency(string parameter)
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateQuery(
            parameter == "grainFactory" ? null! : Substitute.For<IGrainFactory>(),
            parameter == "options" ? null! : OptionsMonitor(),
            parameter == "apiOptions" ? null! : Options.Create(new LatticeApiStateOptions()),
            parameter == "services" ? null! : new ServiceCollection().BuildServiceProvider(),
            parameter == "tenantResolver" ? null! : new NullTenantContextResolver()));

        Assert.That(ex!.ParamName, Is.EqualTo(parameter));
    }

    [TestCase("grainFactory")]
    [TestCase("options")]
    [TestCase("apiOptions")]
    [TestCase("services")]
    public void LatticeStateObserver_constructor_rejects_a_null_dependency(string parameter)
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateObserver(
            parameter == "grainFactory" ? null! : Substitute.For<IGrainFactory>(),
            parameter == "options" ? null! : OptionsMonitor(),
            parameter == "apiOptions" ? null! : Options.Create(new LatticeApiStateOptions()),
            parameter == "services" ? null! : new ServiceCollection().BuildServiceProvider()));

        Assert.That(ex!.ParamName, Is.EqualTo(parameter));
    }

    [Test]
    public void LatticeStateMetricsObserver_constructor_rejects_a_null_sampler()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateMetricsObserver(null!));

        Assert.That(ex!.ParamName, Is.EqualTo("sampler"));
    }

    [TestCase("query")]
    [TestCase("apiOptions")]
    [TestCase("services")]
    public void SharedMetricsSampler_constructor_rejects_a_null_dependency(string parameter)
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new SharedMetricsSampler(
            parameter == "query" ? null! : Substitute.For<ILatticeStateQuery>(),
            parameter == "apiOptions" ? null! : Options.Create(new LatticeApiStateOptions()),
            parameter == "services" ? null! : new ServiceCollection().BuildServiceProvider()));

        Assert.That(ex!.ParamName, Is.EqualTo(parameter));
    }

    [Test]
    public void LatticeStateVisibilityFilter_public_constructor_rejects_null_services()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateVisibilityFilter(
            null!,
            new LatticeApiStateOptions()));

        Assert.That(ex!.ParamName, Is.EqualTo("services"));
    }

    [Test]
    public void LatticeStateVisibilityFilter_public_constructor_rejects_null_options()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateVisibilityFilter(
            new ServiceCollection().BuildServiceProvider(),
            null!));

        Assert.That(ex!.ParamName, Is.EqualTo("options"));
    }

    [Test]
    public void LatticeStateVisibilityFilter_direct_constructor_rejects_null_gate()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => _ = new LatticeStateVisibilityFilter(
            null!,
            membership: null,
            LatticeStateApiReadVisibility.Auto));

        Assert.That(ex!.ParamName, Is.EqualTo("gate"));
    }

    private static LatticeStateQuery CreateQuery(
        IServiceProvider? services = null,
        IGrainFactory? grainFactory = null,
        LatticeApiStateOptions? apiOptions = null)
    {
        var options = OptionsMonitor();
        return new LatticeStateQuery(
            grainFactory ?? Substitute.For<IGrainFactory>(),
            options,
            Options.Create(apiOptions ?? new LatticeApiStateOptions()),
            services ?? new ServiceCollection().BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    private static LatticeStateObserver CreateObserver() => new(
        Substitute.For<IGrainFactory>(),
        OptionsMonitor(),
        Options.Create(new LatticeApiStateOptions()),
        new ServiceCollection().BuildServiceProvider());

    private static IOptionsMonitor<LatticeOptions> OptionsMonitor()
    {
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return options;
    }

    private static (string? TreeId, string Key) DecodeTagMemberToken(string? token)
    {
        var args = new object?[] { token, null, null! };
        InvokeStatic("DecodeTagMemberToken", args);
        return ((string?)args[1], (string)args[2]!);
    }

    private static int CompareTaggedKey(string leftTree, string leftKey, string rightTree, string rightKey) =>
        InvokeStatic<int>("CompareTaggedKey", leftTree, leftKey, rightTree, rightKey);

    private static int DecodeOffset(string? token) => InvokeStatic<int>("DecodeOffset", token);

    private static EntryHistoryBound MapHistoryBound(EntryHistoryPage page) =>
        InvokeStatic<EntryHistoryBound>("MapHistoryBound", page);

    private static EntryRevisionRecord MapRevision(EntryRevision revision, int previewBudget) =>
        InvokeStatic<EntryRevisionRecord>(
            "MapRevision",
            "tree",
            revision,
            previewBudget,
            null,
            CrdtProvenanceDecoderRegistry.Default);

    private static IReadOnlyList<CrdtMemberChange> DecodeMemberChanges(
        EntryRevision revision,
        CrdtShapeRegistry? shapeRegistry,
        CrdtProvenanceDecoderRegistry? decoderRegistry = null) =>
        InvokeStatic<IReadOnlyList<CrdtMemberChange>>(
            "DecodeMemberChanges",
            "tree",
            revision,
            shapeRegistry,
            decoderRegistry ?? CrdtProvenanceDecoderRegistry.Default);

    private static CrdtShape ThrowingShape(LatticeMergeMode mode) => new(
        mode,
        _ => throw new FormatException("bad state"),
        _ => throw new FormatException("bad delta"),
        (_, _) => { },
        (_, _) => { },
        () => new object(),
        _ => Array.Empty<byte>());

    private static WalShardPage WalPage(params WalShardSequencedEntry[] entries) => new()
    {
        Entries = entries,
        NextSequence = entries.Length == 0 ? 0 : entries[^1].Sequence + 1,
    };

    private static LatticeStateObserver CreateObserverWithWal(
        LatticeApiStateOptions apiOptions,
        WalShardPage page,
        long nextSequence = 0,
        long liveEntries = 0)
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync("tree").Returns(Task.FromResult("tree"));
        registry.GetEntryAsync("tree").Returns(Task.FromResult<Orleans.Lattice.BPlusTree.State.TreeRegistryEntry?>(null));

        var wal = Substitute.For<IWalShardGrain>();
        wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<long>(nextSequence));
        wal.GetLiveEntryCountAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(liveEntries));
        wal.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<WalShardPage>(page));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree").Returns(tree);
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        grainFactory.GetGrain<IWalShardGrain>("tree/0").Returns(wal);

        return new LatticeStateObserver(
            grainFactory,
            OptionsMonitor(),
            Options.Create(apiOptions),
            new ServiceCollection().BuildServiceProvider());
    }

    private static bool TryProject(
        LatticeStateObserver observer,
        StateObserveRequest request,
        WalRecord record,
        out StateChangeKind kind)
    {
        var args = new object?[] { request, record, null };
        var result = InvokeInstance<bool>(observer, "TryProject", args);
        kind = (StateChangeKind)args[2]!;
        return result;
    }

    private static void InvokeStatic(string name, params object?[] args)
    {
        var method = typeof(LatticeStateQuery).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        Invoke(method, null, args);
    }

    private static T InvokeStatic<T>(string name, params object?[] args)
    {
        var method = typeof(LatticeStateQuery).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        return (T)Invoke(method, null, args)!;
    }

    private static async Task<T> InvokeStaticAsync<T>(string name, params object?[] args)
    {
        var method = typeof(LatticeStateQuery).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        var task = (Task<T>)Invoke(method, null, args)!;
        return await task.ConfigureAwait(false);
    }

    private static T InvokeObserverStatic<T>(string name, params object?[] args)
    {
        var method = typeof(LatticeStateObserver).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        return (T)Invoke(method, null, args)!;
    }

    private static T InvokeInstance<T>(object instance, string name, params object?[] args)
    {
        var method = instance.GetType().GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        return (T)Invoke(method, instance, args)!;
    }

    private static async Task<T> InvokeInstanceAsync<T>(object instance, string name, params object?[] args)
    {
        var method = instance.GetType().GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        var result = Invoke(method, instance, args)!;
        if (result is Task<T> task)
        {
            return await task.ConfigureAwait(false);
        }

        return await ((dynamic)result).AsTask().ConfigureAwait(false);
    }

    private static object? Invoke(MethodInfo method, object? target, object?[] args)
    {
        try
        {
            return method.Invoke(target, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            throw ex.InnerException;
        }
    }

    private static async Task InvokeObserverStaticTask(string name, params object?[] args)
    {
        var method = typeof(LatticeStateObserver).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        await ((Task)Invoke(method, null, args)!).ConfigureAwait(false);
    }

    private static T InvokeSamplerStatic<T>(string name, params object?[] args)
    {
        var method = typeof(SharedMetricsSampler).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        return (T)Invoke(method, null, args)!;
    }

    private static async Task InvokeSamplerStaticTask(string name, params object?[] args)
    {
        var method = typeof(SharedMetricsSampler).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        await ((Task)Invoke(method, null, args)!).ConfigureAwait(false);
    }

    private static bool SameMetrics(TreeMetrics left, TreeMetrics right) =>
        InvokeMetricsObserverStatic<bool>("SameMetrics", left, right);

    private static T InvokeMetricsObserverStatic<T>(string name, params object?[] args)
    {
        var method = typeof(LatticeStateMetricsObserver).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method {name} was not found.");
        return (T)Invoke(method, null, args)!;
    }

    private static IServiceProvider ServicesWithCatalog(
        IViewCatalog catalog,
        ILatticeMergeModeResolver? resolver = null,
        ILatticeAccessGate? accessGate = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(catalog);
        services.AddSingleton<IReadOnlyList<StartupViewRegistration>>(Array.Empty<StartupViewRegistration>());
        if (resolver is not null)
        {
            services.AddSingleton(resolver);
        }

        if (accessGate is not null)
        {
            services.AddSingleton(accessGate);
            services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(NamedSubject));
        }

        return services.BuildServiceProvider();
    }

    private static IServiceProvider VisibilityServices(Func<string, bool> allow)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new AllowMatchingGate(allow));
        services.AddSingleton<ILatticeMembershipContext>(new FixedMembership(NamedSubject));
        return services.BuildServiceProvider();
    }

    private static IServiceProvider VisibilityServicesWithoutMembership()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new AllowMatchingGate(_ => true));
        return services.BuildServiceProvider();
    }

    private static RuntimeViewRegistration RuntimeView(string viewName, string sourceTreeId) => new()
    {
        ViewName = viewName,
        SourceTreeId = sourceTreeId,
        ProjectionTypeName = typeof(LatticeStateApiEdgeCaseTests).FullName!,
        ProjectionVersion = "v1",
    };

    private static ViewRegistration Registration(string viewName, string sourceTreeId, bool aggregation = false)
    {
        if (aggregation)
        {
            var aggregationProjection = Substitute.For<ILatticeAggregationProjection>();
            aggregationProjection.ProjectionVersion.Returns("v1");
            return new ViewRegistration(viewName, sourceTreeId, Projection: null, aggregationProjection);
        }

        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("v1");
        return new ViewRegistration(viewName, sourceTreeId, projection);
    }

    private sealed class FixedViewCatalog(params ViewRegistration[] registrations) : IViewCatalog
    {
        private readonly Dictionary<string, ViewRegistration> _registrations = registrations.ToDictionary(
            static registration => registration.ViewName,
            StringComparer.Ordinal);

        public void Register(ViewRegistration registration) => _registrations[registration.ViewName] = registration;

        public ViewRegistration? TryGet(string viewName) =>
            _registrations.TryGetValue(viewName, out var registration) ? registration : null;

        public void Remove(string viewName) => _registrations.Remove(viewName);

        public IReadOnlyCollection<ViewRegistration> All() => _registrations.Values.ToArray();
    }

    private sealed class ThrowingViewCatalog : IViewCatalog
    {
        public void Register(ViewRegistration registration) => throw new NotSupportedException();

        public ViewRegistration? TryGet(string viewName) => throw new InvalidOperationException("catalog unavailable");

        public void Remove(string viewName) => throw new NotSupportedException();

        public IReadOnlyCollection<ViewRegistration> All() => throw new NotSupportedException();
    }

    private sealed class AllowMatchingGate(Func<string, bool> allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(allow(request.TreeId) ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("hidden"));
    }

    private sealed class PrefixGate(string prefix) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Filtered(key => key.StartsWith(prefix, StringComparison.Ordinal)));
    }

    private sealed class FixedMembership(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(subject);
    }
}
