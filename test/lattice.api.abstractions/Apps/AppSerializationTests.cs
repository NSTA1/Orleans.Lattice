using System.Collections.Immutable;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

[TestFixture]
public sealed class AppSerializationTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void SetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void TearDown() => _services.Dispose();

    private static readonly AppRoleBindingDescriptor Binding = new() { RoleName = "reader", GroupId = "group-42" };
    private static readonly AppProvenanceDescriptor Provenance = new()
    {
        Source = "in-image", Publisher = "first-party", Reference = "assembly:example",
    };
    private static readonly AppRoleScope RoleScope = new()
    {
        Tree = "orders", App = "sales", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "open/",
    };
    private static readonly AppExceptionScope ExceptionScope = new()
    {
        App = "sales", Tree = "orders", Kind = LatticeScopeKind.Key, KeyOrPrefix = "order-42",
    };
    private static readonly AppCapabilityCeilingDescriptor Ceiling = new()
    {
        AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead,
        ApprovedExceptionScopes =
        [
            ExceptionScope,
            new() { AdoptedTreeId = "legacy-orders", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "open/" },
        ],
    };
    private static readonly AppTreeDescriptor Tree = new()
    {
        Name = "orders", Rebuildable = true, AdoptedTreeId = "legacy-orders",
        ShardCount = 4, VirtualShardCount = 64, MaxLeafKeys = 128,
        MaxInternalChildren = 16, WalPartitions = 8, SoftDeleteDuration = TimeSpan.FromDays(3),
    };
    private static readonly AppRoleDescriptor Role = new()
    {
        Name = "reader", Operations = LatticeOperation.Read | LatticeOperation.RangeRead, Scopes = [RoleScope],
    };
    private static readonly AppSubscriptionDescriptor Subscription = new()
    {
        Name = "new-orders", Tree = "orders", App = "sales", KeyPrefix = "open/",
    };
    private static readonly AppMcpToolDescriptor Tool = new()
    {
        Name = "find_orders", Description = "Find open orders", Role = "reader",
    };
    private static readonly AppReplicationDescriptor Replication = new()
    {
        Tree = "orders", MergeMode = LatticeMergeMode.OrSet,
    };
    private static readonly AppSchemaDescriptor Schema = new()
    {
        Tree = "orders", Family = "order", Version = 3, StrictIngest = true,
    };
    private static readonly AppSummary Summary = new()
    {
        Slug = "inventory", Version = "1.2.3-beta.1+build.42", State = AppLifecycleState.Disabled, Provenance = Provenance,
    };
    private static readonly AppDescriptor Descriptor = new()
    {
        Slug = Summary.Slug, Version = Summary.Version, State = AppLifecycleState.Installed, Provenance = Provenance,
        Ceiling = Ceiling, RoleBindings = [Binding], Trees = [Tree], Roles = [Role],
        Subscriptions = [Subscription], McpTools = [Tool], Replication = [Replication], Schema = [Schema],
    };

    private static IEnumerable<object> Samples()
    {
        yield return Binding;
        yield return Provenance;
        yield return RoleScope;
        yield return ExceptionScope;
        yield return Ceiling;
        yield return Tree;
        yield return Role;
        yield return Subscription;
        yield return Tool;
        yield return Replication;
        yield return Schema;
        yield return Summary;
        yield return Descriptor;
        yield return new AppCatalog { Apps = [Summary] };
        yield return new AppInstallRequest
        {
            Slug = Summary.Slug, Version = Summary.Version, RoleBindings = [Binding], Ceiling = Ceiling,
        };
        yield return new AppLifecycleResult
        {
            Slug = Summary.Slug, Version = Summary.Version, State = AppLifecycleState.Enabled, Changed = true,
        };
        yield return new AppConsentUpdate { Slug = Summary.Slug, Version = Summary.Version, Ceiling = Ceiling };
        yield return new AppConsentReport { Slug = Summary.Slug, Version = Summary.Version, Ceiling = Ceiling };
        yield return new LatticeAppsCapabilities
        {
            CanInstall = true, CanEnable = true, CanDisable = true, CanUninstall = true,
            CanList = true, CanDescribe = true, CanGetConsent = true, CanUpdateConsent = true,
        };
    }

    private static IEnumerable<TestCaseData> RoundTripCases() => Samples()
        .Select(sample => new TestCaseData(sample).SetName($"Round_trip_preserves_all_{sample.GetType().Name}_members"));

    [TestCaseSource(nameof(RoundTripCases))]
    public void Round_trip_preserves_populated_members(object original)
    {
        var copy = _serializer.Deserialize<object>(_serializer.SerializeToArray(original));
        Assert.That(copy.GetType(), Is.EqualTo(original.GetType()));
        Assert.That(JsonSerializer.Serialize(copy, copy.GetType()),
            Is.EqualTo(JsonSerializer.Serialize(original, original.GetType())));
    }

    [Test]
    public void Round_trip_cases_cover_every_app_dto()
    {
        var dtoTypes = typeof(ILatticeAppsControl).Assembly.GetTypes()
            .Where(t => t.Namespace == typeof(ILatticeAppsControl).Namespace
                && t.IsDefined(typeof(GenerateSerializerAttribute), false));
        Assert.That(Samples().Select(s => s.GetType()), Is.EquivalentTo(dtoTypes));
        Assert.That(Samples().Count(), Is.EqualTo(19));
    }

    [TestCaseSource(nameof(LifecycleStates))]
    public void Round_trip_preserves_every_lifecycle_state(AppLifecycleState state)
    {
        var result = new AppDescriptor
        {
            Slug = "inventory", Version = "1.0.0", Provenance = Provenance, State = state,
        };
        var copy = _serializer.Deserialize<AppDescriptor>(_serializer.SerializeToArray(result));
        Assert.That(copy.State, Is.EqualTo(state));
    }

    private static IEnumerable<AppLifecycleState> LifecycleStates() => Enum.GetValues<AppLifecycleState>();

    [TestCase(LatticeScopeKind.Tree, null)]
    [TestCase(LatticeScopeKind.Key, "one")]
    [TestCase(LatticeScopeKind.Prefix, "prefix/")]
    public void Round_trip_preserves_self_app_scopes(LatticeScopeKind kind, string? key)
    {
        var scope = new AppRoleScope { Tree = "orders", Kind = kind, KeyOrPrefix = key };
        var copy = _serializer.Deserialize<AppRoleScope>(_serializer.SerializeToArray(scope));
        Assert.That(copy, Is.EqualTo(scope));
        Assert.That(copy.App, Is.Null);
    }

    [Test]
    public void Describe_before_install_carries_no_consent_or_binding_and_keeps_requested_capabilities()
    {
        var available = Descriptor with
        {
            State = AppLifecycleState.NotInstalled, Ceiling = null, RoleBindings = [],
            Trees = [new() { Name = "orders" }],
        };
        var copy = _serializer.Deserialize<AppDescriptor>(_serializer.SerializeToArray(available));
        Assert.Multiple(() =>
        {
            Assert.That(copy.State, Is.EqualTo(AppLifecycleState.NotInstalled));
            Assert.That(copy.Ceiling, Is.Null);
            Assert.That(copy.RoleBindings, Is.Empty);
            Assert.That(copy.Trees.Single().AdoptedTreeId, Is.Null);
            Assert.That(copy.Trees.Single().SoftDeleteDuration, Is.Null);
            Assert.That(copy.Roles.Single().Scopes.Single(), Is.EqualTo(RoleScope));
            Assert.That(copy.Subscriptions.Single(), Is.EqualTo(Subscription));
            Assert.That(copy.McpTools.Single(), Is.EqualTo(Tool));
        });
    }

    [Test]
    public void Defaults_deny_capabilities_and_keep_collections_empty()
    {
        var capabilities = new LatticeAppsCapabilities();
        var copy = _serializer.Deserialize<LatticeAppsCapabilities>(_serializer.SerializeToArray(capabilities));
        Assert.That(typeof(LatticeAppsCapabilities).GetProperties().Select(p => p.GetValue(copy)),
            Is.All.EqualTo(false));
        Assert.Multiple(() =>
        {
            Assert.That(new AppCapabilityCeilingDescriptor().AllowedOperations, Is.EqualTo(LatticeOperation.None));
            Assert.That(new AppCapabilityCeilingDescriptor().ApprovedExceptionScopes, Is.Empty);
            Assert.That(new AppCatalog().Apps, Is.Empty);
        });
    }

    [Test]
    public void Dtos_are_deeply_immutable_records_with_sequential_field_ids()
    {
        foreach (var type in Samples().Select(s => s.GetType()))
        {
            Assert.That(type.IsSealed, Is.True, type.Name);
            Assert.That(type.IsDefined(typeof(ImmutableAttribute), false), Is.True, type.Name);
            var properties = type.GetProperties();
            Assert.That(properties.Select(p => p.GetCustomAttribute<IdAttribute>()?.Id).Order(),
                Is.EqualTo(Enumerable.Range(0, properties.Length).Select(i => (uint?)i)), type.Name);
            foreach (var property in properties)
            {
                var propertyType = property.PropertyType;
                Assert.That(propertyType.IsArray, Is.False, $"{type.Name}.{property.Name}");
                if (propertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(propertyType))
                    Assert.That(propertyType.IsGenericType
                        && propertyType.GetGenericTypeDefinition() == typeof(ImmutableArray<>), Is.True, property.Name);
                Assert.That(property.SetMethod!.ReturnParameter.GetRequiredCustomModifiers(),
                    Does.Contain(typeof(System.Runtime.CompilerServices.IsExternalInit)), property.Name);
            }
        }
    }
}
