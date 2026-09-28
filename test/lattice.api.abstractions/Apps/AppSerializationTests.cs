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

    private static IEnumerable<object> Samples() => PreEpicSamples().Concat(EpicSamples());

    /// <summary>
    /// The nineteen app DTO samples as they existed before epic #3807, in a fixed order.
    /// <see cref="AppPreEpicWireCompatibilityTests"/> holds the bytes these produced on the
    /// pre-epic types, so the order and content here must not change.
    /// </summary>
    internal static IEnumerable<object> PreEpicSamples()
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

    internal static readonly AppIconDescriptor Icon = new() { Path = "icons/app.svg", Sha256 = new string('a', 64) };
    internal static readonly AppPresentationDescriptor Presentation = new()
    {
        DisplayName = "Inventory", Summary = "Tracks stock", Description = "Line one\nLine two",
        Icon = Icon, Categories = ["operations", "stock"], DocumentationUrl = "https://example.test/docs",
        PublisherDisplayName = "Example Ltd",
    };
    internal static readonly AppUiScriptDescriptor Script = new() { Path = "app.js", Module = true };
    internal static readonly AppUiAssetDescriptor Asset = new()
    {
        Path = "app.js", MediaType = "text/javascript", Sha256 = new string('b', 64),
    };
    internal static readonly AppUiBridgeGrantDescriptor ReadOrders = new() { Operation = "data.read", Tree = "orders" };
    internal static readonly AppUiBridgeGrantDescriptor Notify = new() { Operation = "ui.notify" };
    internal static readonly AppUiDescriptor Ui = new()
    {
        Entry = "index.html", Styles = ["app.css"], Scripts = [Script], Assets = [Asset],
        BundleDigest = new string('c', 64), Bridge = [ReadOrders, Notify], MinProtocol = 1,
    };
    private static readonly WorkspaceTreeDescriptor WorkspaceTree = new()
    {
        Name = "orders", Rebuildable = true, Adopted = true, ShardCount = 4, VirtualShardCount = 64,
        MaxLeafKeys = 128, MaxInternalChildren = 16, WalPartitions = 8, SoftDeleteDuration = TimeSpan.FromDays(3),
    };
    private static readonly AvailableAppSummary Available = new()
    {
        SourceKey = "in-image", Slug = "inventory", NewestVersion = "1.3.0", AvailableVersions = ["1.3.0", "1.2.3"],
        Presentation = Presentation, HasUi = true, InstalledVersion = "1.2.3", InstalledState = AppLifecycleState.Enabled,
    };
    private static readonly AppBridgeValue BridgeValue = new() { Key = "order-42", Value = new byte[] { 1, 2, 3 } };

    private static IEnumerable<object> EpicSamples()
    {
        yield return Descriptor with { Presentation = Presentation, Ui = Ui, SourceKey = "in-image" };
        yield return new AppInstallRequest
        {
            Slug = Summary.Slug, Version = Summary.Version, Ceiling = Ceiling, SourceKey = "in-image",
        };
        yield return new AppConsentUpdate
        {
            Slug = Summary.Slug, Version = Summary.Version, Ceiling = Ceiling, BridgeGrants = [ReadOrders, Notify],
        };
        yield return new AppConsentReport
        {
            Slug = Summary.Slug, Version = Summary.Version, Ceiling = Ceiling, BridgeGrants = [ReadOrders, Notify],
        };
        yield return new AppSourceSummary
        {
            Key = "feed", DisplayName = "Package feed", Kind = AppSourceSummaryKind.Dynamic,
            Capabilities = AppSourceSummaryCapabilities.Enumerate | AppSourceSummaryCapabilities.Search
                | AppSourceSummaryCapabilities.MultipleVersions | AppSourceSummaryCapabilities.RequiresAcquisition,
        };
        yield return Presentation;
        yield return Icon;
        yield return Ui;
        yield return ReadOrders;
        yield return Notify;
        yield return Script;
        yield return Asset;
        yield return new AppIconAsset { Bytes = new byte[] { 0x3c, 0x73, 0x76, 0x67 }, MediaType = "image/svg+xml", Sha256 = Icon.Sha256 };
        yield return new AppUiAsset { Path = "app.js", Bytes = new byte[] { 0x2f, 0x2f }, MediaType = "text/javascript", Sha256 = Asset.Sha256 };
        yield return new AvailableAppQuery
        {
            SourceKey = "in-image", Text = "stock", Filter = AvailableAppFilter.Updates, PageSize = 25, Continuation = "cursor",
        };
        yield return Available;
        yield return new AvailableAppPage { Apps = [Available], Continuation = "next" };
        yield return new WorkspaceAppSummary
        {
            Slug = "inventory", Version = "1.2.3", InstallRevision = 7, Presentation = Presentation, HasUi = true,
            Roles = ["reader"],
        };
        yield return WorkspaceTree;
        yield return new WorkspaceAppDescriptor
        {
            Slug = "inventory", Version = "1.2.3", InstallRevision = 7, SourceKey = "in-image",
            State = AppLifecycleState.Enabled, Presentation = Presentation, Trees = [WorkspaceTree], Roles = [Role],
            McpTools = [Tool], Subscriptions = [Subscription], Replication = [Replication], Ui = Ui,
        };
        yield return new AppBridgeTarget { AppSlug = "inventory", InstallRevision = 7, LogicalTree = "orders" };
        yield return BridgeValue;
        yield return new AppBridgePage { Entries = [BridgeValue], Continuation = "next" };
        yield return new LatticeAppCatalogCapabilities
        {
            CanListSources = true, CanListAvailable = true, CanDescribeFromSource = true, CanGetIcon = true,
        };
    }

    private static IEnumerable<TestCaseData> RoundTripCases() => Samples()
        .Select((sample, index) => new TestCaseData(sample)
            .SetName($"Round_trip_preserves_all_{sample.GetType().Name}_members_{index:00}"));

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
                && t.IsDefined(typeof(GenerateSerializerAttribute), false)
                && !typeof(Exception).IsAssignableFrom(t));
        Assert.That(Samples().Select(s => s.GetType()).Distinct(), Is.EquivalentTo(dtoTypes));
        Assert.That(PreEpicSamples().Count(), Is.EqualTo(19));
        Assert.That(Samples().Select(s => s.GetType()).Distinct().Count(), Is.EqualTo(38));
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
