using System.Collections.Immutable;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

/// <summary>
/// Pins the epic #3807 app DTOs: their defaults, closed enums, the workspace projection's
/// exclusions, the bridge operation vocabulary, and compact byte transport.
/// </summary>
[TestFixture]
public sealed class AppEpicModelTests
{
    // The canonical bridge operation vocabulary is owned by AppUiBridgeOperations in
    // Orleans.Lattice.Apps (#3808); the transport is pinned against it, so the two can never drift.
    private static readonly string[] BridgeVocabulary = [.. Orleans.Lattice.Apps.AppUiBridgeOperations.All.Order(StringComparer.Ordinal)];

    private static readonly Type[] EpicDtoTypes =
    [
        typeof(AppSourceSummary), typeof(AppPresentationDescriptor), typeof(AppIconDescriptor),
        typeof(AppUiDescriptor), typeof(AppUiScriptDescriptor), typeof(AppUiAssetDescriptor),
        typeof(AppUiBridgeGrantDescriptor),
        typeof(AppIconAsset), typeof(AppUiAsset), typeof(AvailableAppQuery), typeof(AvailableAppPage),
        typeof(AvailableAppSummary), typeof(WorkspaceAppSummary), typeof(WorkspaceAppDescriptor),
        typeof(WorkspaceTreeDescriptor), typeof(AppBridgeTarget), typeof(AppBridgeValue), typeof(AppBridgePage),
        typeof(LatticeAppCatalogCapabilities),
    ];

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

    [TestCaseSource(nameof(EpicDtoTypes))]
    public void Collections_default_to_empty_and_never_to_a_default_array(Type type)
    {
        var instance = Activator.CreateInstance(type)!;

        foreach (var property in type.GetProperties().Where(p => p.PropertyType == typeof(ReadOnlyMemory<byte>)))
            Assert.That(((ReadOnlyMemory<byte>)property.GetValue(instance)!).Length, Is.Zero, $"{type.Name}.{property.Name}");

        foreach (var property in type.GetProperties().Where(p => p.PropertyType.IsGenericType
                     && p.PropertyType.GetGenericTypeDefinition() == typeof(ImmutableArray<>)))
        {
            var value = property.GetValue(instance)!;
            var isDefault = (bool)property.PropertyType.GetProperty("IsDefault")!.GetValue(value)!;
            var length = (int)property.PropertyType.GetProperty("Length")!.GetValue(value)!;
            Assert.That(isDefault, Is.False, $"{type.Name}.{property.Name}");
            Assert.That(length, Is.Zero, $"{type.Name}.{property.Name}");
        }
    }

    [Test]
    public void Optional_members_default_to_null_or_denied()
    {
        var capabilities = new LatticeAppCatalogCapabilities();
        var copy = _serializer.Deserialize<LatticeAppCatalogCapabilities>(_serializer.SerializeToArray(capabilities));
        var available = new AvailableAppSummary { SourceKey = "in-image", Slug = "inventory", NewestVersion = "1.0.0" };

        Assert.Multiple(() =>
        {
            Assert.That(typeof(LatticeAppCatalogCapabilities).GetProperties().Select(p => p.GetValue(copy)),
                Is.All.EqualTo(false));
            Assert.That(available.Presentation, Is.Null);
            Assert.That(available.HasUi, Is.False);
            Assert.That(available.InstalledVersion, Is.Null);
            Assert.That(available.InstalledState, Is.Null);
            Assert.That(new AvailableAppPage().Continuation, Is.Null);
            Assert.That(new AppBridgePage().Continuation, Is.Null);
            Assert.That(new AppPresentationDescriptor { DisplayName = "x" }.Icon, Is.Null);
            Assert.That(new WorkspaceAppDescriptor { Slug = "a", Version = "1.0.0" }.Ui, Is.Null);
            Assert.That(new AppDescriptor
            {
                Slug = "a", Version = "1.0.0",
                Provenance = new() { Source = "in-image", Publisher = "first-party" },
            }.SourceKey, Is.Null);
        });
    }

    [Test]
    public void Available_query_defaults_to_every_source_and_the_default_page()
    {
        var query = new AvailableAppQuery();

        Assert.Multiple(() =>
        {
            Assert.That(query.SourceKey, Is.Null);
            Assert.That(query.Text, Is.Null);
            Assert.That(query.Filter, Is.EqualTo(AvailableAppFilter.All));
            Assert.That(query.PageSize, Is.EqualTo(AvailableAppQuery.DefaultPageSize));
            Assert.That(query.Continuation, Is.Null);
            Assert.That(AvailableAppQuery.DefaultPageSize, Is.EqualTo(50));
            Assert.That(AvailableAppQuery.MaxPageSize, Is.EqualTo(200));
        });
    }

    [Test]
    public void Enums_are_closed_and_their_values_are_pinned()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Enum.GetValues<AvailableAppFilter>().Select(v => $"{v}={(int)v}"),
                Is.EqualTo(new[] { "All=0", "Installed=1", "Available=2", "Updates=3" }));
            Assert.That(Enum.GetValues<AppSourceSummaryKind>().Select(v => $"{v}={(int)v}"),
                Is.EqualTo(new[] { "Static=0", "Dynamic=1" }));
            Assert.That(Enum.GetValues<AppSourceSummaryCapabilities>().Select(v => $"{v}={(int)v}"),
                Is.EqualTo(new[] { "None=0", "Enumerate=1", "Search=2", "MultipleVersions=4", "RequiresAcquisition=8" }));
            Assert.That(typeof(AppSourceSummaryCapabilities).IsDefined(typeof(FlagsAttribute), false), Is.True);
        });
    }

    [TestCase(AvailableAppFilter.All)]
    [TestCase(AvailableAppFilter.Installed)]
    [TestCase(AvailableAppFilter.Available)]
    [TestCase(AvailableAppFilter.Updates)]
    public void Query_round_trip_preserves_every_filter(AvailableAppFilter filter)
    {
        var copy = _serializer.Deserialize<AvailableAppQuery>(
            _serializer.SerializeToArray(new AvailableAppQuery { Filter = filter }));

        Assert.That(copy, Is.EqualTo(new AvailableAppQuery { Filter = filter }));
    }

    [TestCaseSource(nameof(LifecycleStates))]
    public void Available_summary_round_trip_preserves_every_installed_state(AppLifecycleState? state)
    {
        var summary = new AvailableAppSummary
        {
            SourceKey = "in-image", Slug = "inventory", NewestVersion = "1.0.0", InstalledState = state,
        };

        var copy = _serializer.Deserialize<AvailableAppSummary>(_serializer.SerializeToArray(summary));

        Assert.That(copy.InstalledState, Is.EqualTo(state));
    }

    private static IEnumerable<AppLifecycleState?> LifecycleStates() =>
        Enum.GetValues<AppLifecycleState>().Select(s => (AppLifecycleState?)s).Append(null);

    [Test]
    public void Workspace_projection_exposes_no_consent_binding_or_physical_tree_member()
    {
        var forbiddenTypes = new[]
        {
            typeof(AppCapabilityCeilingDescriptor), typeof(AppExceptionScope), typeof(AppRoleBindingDescriptor),
            typeof(AppConsentReport), typeof(AppTreeDescriptor), typeof(AppProvenanceDescriptor),
        };
        var forbiddenNames = new[] { "TreeId", "Ceiling", "Binding", "Consent", "Exception", "Physical", "Group" };

        var reached = Reachable(typeof(WorkspaceAppDescriptor), typeof(WorkspaceAppSummary));

        Assert.Multiple(() =>
        {
            Assert.That(reached, Does.Contain(typeof(AppRoleScope)), "the walk must reach nested scope templates");
            Assert.That(reached.Intersect(forbiddenTypes), Is.Empty);
            foreach (var property in reached.SelectMany(t => t.GetProperties()))
                Assert.That(forbiddenNames.Where(n => property.Name.Contains(n, StringComparison.Ordinal)), Is.Empty,
                    $"{property.DeclaringType!.Name}.{property.Name}");
        });
    }

    [Test]
    public void Workspace_tree_reports_adoption_as_a_flag_only()
    {
        var adopted = typeof(WorkspaceTreeDescriptor).GetProperty(nameof(WorkspaceTreeDescriptor.Adopted))!;

        Assert.Multiple(() =>
        {
            Assert.That(adopted.PropertyType, Is.EqualTo(typeof(bool)));
            Assert.That(typeof(WorkspaceTreeDescriptor).GetProperties().Where(p => p.PropertyType == typeof(string))
                .Select(p => p.Name), Is.EqualTo(new[] { "Name" }));
        });
    }

    [Test]
    public void Bridge_vocabulary_is_the_seven_canonical_operations()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BridgeVocabulary, Is.Unique);
            Assert.That(BridgeVocabulary, Has.Length.EqualTo(7));
            Assert.That(BridgeVocabulary, Is.All.Matches(@"^[a-z]+\.[a-z]+$"));
        });
    }

    [Test]
    public void Every_bridge_operation_is_transported_verbatim_with_and_without_a_tree()
    {
        ImmutableArray<AppUiBridgeGrantDescriptor> grants =
        [
            .. BridgeVocabulary.Select(op => new AppUiBridgeGrantDescriptor { Operation = op }),
            .. BridgeVocabulary.Select(op => new AppUiBridgeGrantDescriptor { Operation = op, Tree = "orders" }),
        ];
        var ui = new AppUiDescriptor { Entry = "index.html", BundleDigest = new string('0', 64), Bridge = grants };
        var consent = new AppConsentUpdate { Slug = "inventory", Version = "1.0.0", Ceiling = new(), BridgeGrants = grants };
        var report = new AppConsentReport { Slug = "inventory", Version = "1.0.0", Ceiling = new(), BridgeGrants = grants };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(ui).Bridge, Is.EqualTo(grants));
            Assert.That(RoundTrip(consent).BridgeGrants!.Value, Is.EqualTo(grants));
            Assert.That(RoundTrip(report).BridgeGrants!.Value, Is.EqualTo(grants));
        });
    }

    [Test]
    public void Bridge_grants_keep_tree_scopes_per_operation()
    {
        // data.read on [a, b] with data.write on [a] only: a flat operation list plus a flat
        // tree list cannot express this, so the grant set must carry one pair per scope.
        ImmutableArray<AppUiBridgeGrantDescriptor> grants =
        [
            new() { Operation = "data.read", Tree = "a" },
            new() { Operation = "data.read", Tree = "b" },
            new() { Operation = "data.write", Tree = "a" },
            new() { Operation = "context.read" },
        ];

        var copy = RoundTrip(new AppUiDescriptor { Entry = "index.html", BundleDigest = new string('0', 64), Bridge = grants });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Bridge.Where(g => g.Operation == "data.write").Select(g => g.Tree), Is.EqualTo(new[] { "a" }));
            Assert.That(copy.Bridge.Where(g => g.Operation == "data.read").Select(g => g.Tree), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(copy.Bridge.Single(g => g.Operation == "context.read").Tree, Is.Null);
        });
    }

    [Test]
    public void Bridge_grant_defaults_to_every_declared_tree_and_compares_by_value()
    {
        var all = new AppUiBridgeGrantDescriptor { Operation = "data.read" };

        Assert.Multiple(() =>
        {
            Assert.That(all.Tree, Is.Null);
            Assert.That(all, Is.EqualTo(new AppUiBridgeGrantDescriptor { Operation = "data.read" }));
            Assert.That(all, Is.Not.EqualTo(all with { Tree = "orders" }));
            Assert.That(new AppUiDescriptor { Entry = "index.html", BundleDigest = new string('0', 64) }.Bridge, Is.Empty);
            Assert.That(typeof(AppUiBridgeGrantDescriptor).GetProperties().Select(p => p.Name),
                Is.EqualTo(new[] { "Operation", "Tree" }));
        });
    }

    [Test]
    public void Consent_update_distinguishes_unchanged_from_an_emptied_bridge_set()
    {
        var unchanged = new AppConsentUpdate { Slug = "inventory", Version = "1.0.0", Ceiling = new() };
        var emptied = unchanged with { BridgeGrants = [] };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(unchanged).BridgeGrants, Is.Null);
            Assert.That(RoundTrip(emptied).BridgeGrants, Is.Not.Null);
            Assert.That(RoundTrip(emptied).BridgeGrants!.Value, Is.Empty);
            Assert.That(new AppConsentReport { Slug = "inventory", Version = "1.0.0", Ceiling = new() }.BridgeGrants, Is.Null);
        });
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(64 * 1024)]
    public void Byte_payloads_round_trip_exactly_and_compactly(int length)
    {
        var bytes = new byte[length];
        new Random(length).NextBytes(bytes);
        var asset = new AppUiAsset
        {
            Path = "app.js", Bytes = bytes, MediaType = "text/javascript", Sha256 = new string('d', 64),
        };

        var payload = _serializer.SerializeToArray(asset);
        var copy = _serializer.Deserialize<AppUiAsset>(payload);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Bytes.Span.SequenceEqual(bytes), Is.True);
            Assert.That(payload.Length, Is.LessThan(length + 256), "bytes must be transported in bulk, not per element");
        });
    }

    [Test]
    public void Bridge_value_round_trip_preserves_bytes()
    {
        var value = new AppBridgeValue { Key = "k", Value = new byte[] { 0, 255, 7 } };

        Assert.That(RoundTrip(value).Value.ToArray(), Is.EqualTo(new byte[] { 0, 255, 7 }));
    }

    private T RoundTrip<T>(T value) => _serializer.Deserialize<T>(_serializer.SerializeToArray(value));

    private static HashSet<Type> Reachable(params Type[] roots)
    {
        var seen = new HashSet<Type>();
        var pending = new Stack<Type>(roots);
        while (pending.TryPop(out var type))
        {
            if (type.Namespace != typeof(ILatticeAppsControl).Namespace || type.IsEnum || !seen.Add(type))
                continue;
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                pending.Push(Unwrap(property.PropertyType));
        }

        return seen;
    }

    private static Type Unwrap(Type type) =>
        Nullable.GetUnderlyingType(type) is { } underlying ? Unwrap(underlying)
        : type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ImmutableArray<>)
            ? Unwrap(type.GetGenericArguments()[0])
            : type;
}
