using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Tests.Predicates;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for <see cref="RuntimeViewRehydrator"/>.
/// </summary>
[TestFixture]
public class RuntimeViewRehydratorTests
{
    private sealed class FakeViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    private sealed class FakeAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) =>
            Array.Empty<AggregationContribution>();
    }

    private sealed class ThrowingViewProjection : ILatticeViewProjection
    {
        public ThrowingViewProjection() => throw new InvalidOperationException("ctor boom");

        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    private sealed class ThrowingVersionViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => throw new InvalidOperationException("version boom");

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    private static IServiceProvider EmptyServices() => new ServiceCollection().BuildServiceProvider();

    private static RuntimeViewProjectionProviderCatalog Providers(
        params RuntimeViewProjectionProviderRegistration[] registrations) => new(registrations);

    private static RuntimeViewRegistration Registration(
        string typeName,
        bool isAggregation,
        bool accumulative = false,
        string projectionVersion = "v1") => new()
    {
        ViewName = "v-name",
        SourceTreeId = "src-tree",
        ProjectionTypeName = typeName,
        ProjectionVersion = projectionVersion,
        IsAggregation = isAggregation,
        Accumulative = accumulative,
    };

    private static RuntimeViewRegistration ProviderRegistration(
        bool isAggregation = false,
        bool accumulative = false,
        string projectionVersion = "v1",
        byte[]? payload = null) => new()
    {
        ViewName = "v-name",
        SourceTreeId = "src-tree",
        ProjectionTypeName = typeof(FakeViewProjection).FullName!,
        ProjectionVersion = projectionVersion,
        IsAggregation = isAggregation,
        Accumulative = accumulative,
        ProjectionProviderKey = "test-provider",
        ProjectionProviderPayload = payload ?? [1, 2, 3],
    };

    [Test]
    public void Resolve_viewProjection_returnsRegistrationWithProjection()
    {
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.ViewName, Is.EqualTo("v-name"));
        Assert.That(result.SourceTreeId, Is.EqualTo("src-tree"));
        Assert.That(result.Projection, Is.InstanceOf<FakeViewProjection>());
        Assert.That(result.AggregationProjection, Is.Null);
    }

    [Test]
    public void Resolve_accumulativeViewProjection_flowsAccumulativeFlag()
    {
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: false, accumulative: true);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Accumulative, Is.True);
    }

    [Test]
    public void Resolve_aggregationProjection_returnsRegistrationWithAggregation()
    {
        var record = Registration(typeof(FakeAggregationProjection).FullName!, isAggregation: true);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.AggregationProjection, Is.InstanceOf<FakeAggregationProjection>());
        Assert.That(result.Projection, Is.Null);
    }

    [Test]
    public void Resolve_unknownTypeName_returnsNull()
    {
        var record = Registration("Not.A.Real.Projection, Nowhere", isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_emptyTypeName_returnsNull()
    {
        var record = Registration(string.Empty, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_kindMismatch_viewProjectionRequestedAsAggregation_returnsNull()
    {
        // The type is a view projection, but the record claims aggregation; the
        // allow-list's kind gate rejects it before construction.
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: true);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_projectionCtorThrows_returnsNull()
    {
        var record = Registration(typeof(ThrowingViewProjection).FullName!, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_legacyProjectionVersionMismatch_returnsNull()
    {
        var record = Registration(
            typeof(FakeViewProjection).FullName!,
            isAggregation: false,
            projectionVersion: "stale");

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_emptyProjectionVersion_returnsNull()
    {
        var record = Registration(
            typeof(FakeViewProjection).FullName!,
            isAggregation: false,
            projectionVersion: string.Empty);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_emptyViewName_doesNotInvokeProvider()
    {
        var invoked = false;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                invoked = true;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));
        var record = ProviderRegistration() with { ViewName = string.Empty };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }

    [Test]
    public void Resolve_emptySourceTreeId_doesNotInvokeProvider()
    {
        var invoked = false;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                invoked = true;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));
        var record = ProviderRegistration() with { SourceTreeId = string.Empty };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }

    [Test]
    public void Resolve_viewTreeSource_doesNotInvokeProvider()
    {
        var invoked = false;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                invoked = true;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));
        var record = ProviderRegistration() with { SourceTreeId = "view-source" };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }

    [Test]
    public void Resolve_keyedProvider_returnsProviderProjectionAndContext()
    {
        LatticeRuntimeViewProjectionContext? received = null;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                received = context;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Projection, Is.InstanceOf<FakeViewProjection>());
            Assert.That(result.ProjectionProviderKey, Is.EqualTo("test-provider"));
            Assert.That(received!.ViewName, Is.EqualTo("v-name"));
            Assert.That(received.SourceTreeId, Is.EqualTo("src-tree"));
            Assert.That(received.Payload, Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void Resolve_builtInPredicateProvider_preservesFilterBehavior()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var codec = new PredicateRuntimeViewProjectionCodec(
            services.GetRequiredService<Serializer<LatticePredicateNode>>());
        var filter = LatticePredicateTranslator.Translate<PredicatePerson>(person => person.Age >= 18);
        var original = new PredicateLatticeViewProjection(filter);
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            PredicateRuntimeViewProjectionCodec.ProviderKey,
            (_, context) => new LatticeViewDefinition(
                context.ViewName,
                new PredicateLatticeViewProjection(codec.Decode(context.PayloadSpan)))));
        var record = ProviderRegistration(
            projectionVersion: original.ProjectionVersion,
            payload: codec.Encode(filter)) with
        {
            ProjectionProviderKey = PredicateRuntimeViewProjectionCodec.ProviderKey,
        };

        var result = RuntimeViewRehydrator.Resolve(
            record, services, providers, NullLogger.Instance);
        var minor = JsonLatticeSerializer<PredicatePerson>.Default.Serialize(
            new PredicatePerson("Bob", 12, true, 0.5, null, null));
        var writes = result!.Projection!.Project(new LatticeMutation
        {
            TreeId = "src-tree",
            Kind = MutationKind.Set,
            Key = "minor",
            Value = minor,
            Timestamp = new HybridLogicalClock { WallClockTicks = 1 },
            Category = MutationCategory.User,
        }).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(result.ProjectionVersion, Is.EqualTo(original.ProjectionVersion));
            Assert.That(writes, Has.Count.EqualTo(1));
            Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
            Assert.That(writes[0].Key, Is.EqualTo("minor"));
        });
    }

    [Test]
    public void Resolve_keyedAggregationProvider_returnsAggregation()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
                new LatticeViewDefinition(context.ViewName, new FakeAggregationProjection())));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(isAggregation: true),
            EmptyServices(),
            providers,
            NullLogger.Instance);

        Assert.That(result!.AggregationProjection, Is.InstanceOf<FakeAggregationProjection>());
    }

    [Test]
    public void Resolve_missingKeyedProvider_doesNotFallBackToType()
    {
        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_providerPayloadWithoutKey_doesNotFallBackToType()
    {
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: false) with
        {
            ProjectionProviderPayload = [1, 2, 3],
        };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_providerKeyWithoutPayload_doesNotInvokeProvider()
    {
        var invoked = false;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                invoked = true;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));
        var record = ProviderRegistration() with
        {
            ProjectionProviderPayload = null,
        };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }

    [Test]
    public void Resolve_emptyProviderKey_returnsNull()
    {
        var record = ProviderRegistration() with
        {
            ProjectionProviderKey = string.Empty,
        };

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), Providers(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderFailure_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, _) => throw new InvalidOperationException("boom")));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderReturnsNull_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, _) => null!));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderWrongName_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, _) => new LatticeViewDefinition("wrong-name", new FakeViewProjection())));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderProjectionVersionThrows_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
                new LatticeViewDefinition(context.ViewName, new ThrowingVersionViewProjection())));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderWrongShape_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
                new LatticeViewDefinition(context.ViewName, new FakeAggregationProjection())));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(), EmptyServices(), providers, NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_keyedProviderWrongVersion_returnsNull()
    {
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
                new LatticeViewDefinition(context.ViewName, new FakeViewProjection())));

        var result = RuntimeViewRehydrator.Resolve(
            ProviderRegistration(projectionVersion: "stale"),
            EmptyServices(),
            providers,
            NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_oversizedProviderPayload_doesNotInvokeProvider()
    {
        var invoked = false;
        var providers = Providers(new RuntimeViewProjectionProviderRegistration(
            "test-provider",
            (_, context) =>
            {
                invoked = true;
                return new LatticeViewDefinition(context.ViewName, new FakeViewProjection());
            }));
        var record = ProviderRegistration(
            payload: new byte[LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes + 1]);

        var result = RuntimeViewRehydrator.Resolve(
            record, EmptyServices(), providers, NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }
}
