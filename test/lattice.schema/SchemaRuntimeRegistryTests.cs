using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Schema.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;
using System.Text.Json;

namespace Orleans.Lattice.Schema.Tests;

[TestFixture]
public sealed class SchemaRuntimeRegistryTests
{
    [Test]
    public async Task A_custom_host_registry_preserves_upcasting_and_does_not_advertise_runtime_registration()
    {
        var host = Substitute.For<ILatticeSchemaRegistry>();
        var runtime = new SchemaRuntimeRegistry(Substitute.For<IGrainFactory>(), host, Transforms, []);
        Assert.That(runtime.IsSupported, Is.False);
        Assert.That(await runtime.ResolveAsync("orders"), Is.SameAs(host));
        Assert.That(async () => await runtime.RegisterAsync("orders", Registration()), Throws.InvalidOperationException);
    }

    [Test]
    public void Runtime_catalog_JSON_round_trip_preserves_descriptors_and_transform()
    {
        var expected = Registration();
        var actual = JsonSerializer.Deserialize<LatticeSchemaRuntimeRegistration>(
            JsonSerializer.Serialize(expected, JsonSerializerOptions.Web), JsonSerializerOptions.Web)!;
        Assert.Multiple(() =>
        {
            Assert.That(actual.Descriptors, Is.EqualTo(expected.Descriptors));
            Assert.That(actual.Upcasters, Is.EqualTo(expected.Upcasters));
        });
    }

    private static readonly ILatticeValueTransformRegistry Transforms = new LatticeValueTransformRegistry([]);

    private static LatticeSchemaRuntimeRegistration Registration() => new()
    {
        Descriptors = [new(42, 1, "orders-v1"), new(42, 2, "orders-v2")],
        Upcasters = [new()
        {
            SchemaId = 42, FromVersion = 1, ToVersion = 2,
            Transform = LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember("v", LatticeValueTransform.Const(LatticeConstant.Integer(2)))),
        }],
    };

    private static LatticeSchemaRuntimeRegistryGrain Grain(FakePersistentState<SchemaRuntimeRegistryState> state,
        ILatticeSchemaRegistry? host = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("runtime-schema", "orders"));
        context.ActivationServices.Returns(new ServiceCollection().BuildServiceProvider());
        return new(context, state, host ?? new LatticeSchemaRegistryBuilder().Build(), Transforms);
    }

    [Test]
    public async Task Host_catalog_size_does_not_consume_the_runtime_budget_or_get_persisted()
    {
        var builder = new LatticeSchemaRegistryBuilder();
        for (uint version = 1; version <= 129; version++) builder.AddSchema(500, version, $"host-v{version}");
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        var host = builder.Build();
        await Grain(state, host).RegisterAsync(Registration());
        Assert.That(state.State.Registration.Descriptors, Has.Length.EqualTo(2));
        var effective = SchemaRuntimeCatalog.Merge(((LatticeSchemaRegistry)host).Describe(),
            state.State.Registration, Transforms, enforceMergedLimit: false);
        Assert.That(effective.Descriptors, Has.Length.EqualTo(131));
    }

    [Test]
    public async Task Register_persists_round_trips_and_resolves_after_new_activation()
    {
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        await Grain(state).RegisterAsync(Registration());
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<SchemaRuntimeRegistryState>>();
        var restored = serializer.Deserialize(serializer.SerializeToArray(state.State));
        var fresh = Grain(new() { State = restored });
        var catalog = await fresh.GetAsync();
        var registry = SchemaRuntimeCatalog.Build(catalog, Transforms);
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(catalog.Descriptors[0].Name, Is.EqualTo("orders-v1"));
            Assert.That(registry.CanUpcast(42, 1, 2), Is.True);
            Assert.That(registry.Upcast(42, 1, 2, "{}"u8.ToArray()), Is.EqualTo("{\"v\":2}"u8.ToArray()));
        });
    }

    [Test]
    public async Task Identical_retry_is_idempotent_conflicts_leave_persisted_catalog_unchanged()
    {
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        var grain = Grain(state);
        await grain.RegisterAsync(Registration());
        await grain.RegisterAsync(Registration());
        Assert.That((await grain.GetAsync()).Descriptors, Has.Length.EqualTo(2));
        Assert.That(async () => await grain.RegisterAsync(new()
        {
            Descriptors = [new(42, 1, "changed")],
        }), Throws.ArgumentException);
        Assert.That((await grain.GetAsync()).Descriptors[0].Name, Is.EqualTo("orders-v1"));
    }

    [Test]
    public async Task A_later_registration_keeps_every_earlier_entry()
    {
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        var grain = Grain(state);
        await grain.RegisterAsync(Registration());
        await grain.RegisterAsync(new()
        {
            Descriptors = [new(42, 3, "orders-v3")],
            Upcasters = [new() { SchemaId = 42, FromVersion = 2, ToVersion = 3, Transform = LatticeValueTransform.Passthrough() }],
        });

        var catalog = await grain.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Descriptors.Select(d => d.Version), Is.EqualTo(new uint[] { 1, 2, 3 }));
            Assert.That(catalog.Upcasters.Select(h => h.FromVersion), Is.EqualTo(new uint[] { 1, 2 }));
            Assert.That(SchemaRuntimeCatalog.Build(catalog, Transforms).CanUpcast(42, 1, 3), Is.True);
        });
    }

    [Test]
    public async Task A_registration_with_any_conflicting_entry_persists_none_of_it()
    {
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        var grain = Grain(state);
        await grain.RegisterAsync(Registration());

        // The new descriptor is valid on its own; the hop collides with the
        // registered v1 to v2 hop, so the whole registration is refused.
        Assert.That(async () => await grain.RegisterAsync(new()
        {
            Descriptors = [new(42, 3, "orders-v3")],
            Upcasters = [new() { SchemaId = 42, FromVersion = 1, ToVersion = 3, Transform = LatticeValueTransform.Passthrough() }],
        }), Throws.ArgumentException);

        var catalog = await grain.GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1), "the refused registration wrote nothing");
            Assert.That(catalog.Descriptors.Select(d => d.Version), Is.EqualTo(new uint[] { 1, 2 }));
            Assert.That(catalog.Upcasters.Select(h => h.ToVersion), Is.EqualTo(new uint[] { 2 }));
        });
    }

    [Test]
    public async Task A_registration_conflicting_with_a_host_declaration_is_refused_and_a_replay_of_one_persists_nothing()
    {
        var expected = Registration();
        var host = new LatticeSchemaRegistryBuilder()
            .AddSchema(42, 1, "orders-v1")
            .AddSchema(42, 2, "orders-v2")
            .AddUpcaster(42, 1, 2, expected.Upcasters[0].Transform!.Value)
            .Build(Transforms);
        var state = new FakePersistentState<SchemaRuntimeRegistryState>();
        var grain = Grain(state, host);

        Assert.That(async () => await grain.RegisterAsync(new() { Descriptors = [new(42, 1, "renamed")] }),
            Throws.ArgumentException);
        Assert.That(async () => await grain.RegisterAsync(new()
        {
            Descriptors = [new(42, 3, "orders-v3")],
            Upcasters = [new() { SchemaId = 42, FromVersion = 1, ToVersion = 3, Transform = LatticeValueTransform.Passthrough() }],
        }), Throws.ArgumentException);
        Assert.That(state.WriteCount, Is.Zero, "a registration refused against the host declarations writes nothing");

        await grain.RegisterAsync(Registration());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Registration.Descriptors, Is.Empty, "declared descriptors are never persisted");
            Assert.That(state.State.Registration.Upcasters, Is.Empty, "declared hops are never persisted");
        });
    }

    [Test]
    public async Task Callers_cannot_mutate_registered_catalog_arrays_or_nested_transform_nodes()
    {
        var grain = Grain(new());
        var input = Registration();
        await grain.RegisterAsync(input);
        input.Descriptors[0] = new(42, 1, "mutated-input");
        input.Upcasters[0].Transform!.Value.Children![0] =
            LatticeValueTransform.DropMember("v");
        var exported = await grain.GetAsync();
        exported.Descriptors[0] = new(42, 1, "mutated-output");
        exported.Upcasters[0].Transform!.Value.Children![0] =
            LatticeValueTransform.DropMember("v");
        var actual = await grain.GetAsync();
        Assert.That(actual.Descriptors[0].Name, Is.EqualTo("orders-v1"));
        Assert.That(SchemaRuntimeCatalog.Build(actual, Transforms).Upcast(42, 1, 2, "{}"u8.ToArray()),
            Is.EqualTo("{\"v\":2}"u8.ToArray()));
    }

    [Test]
    public async Task Failed_persistence_rolls_back_the_live_catalog()
    {
        var state = new FakePersistentState<SchemaRuntimeRegistryState> { ThrowOnWrite = new IOException("storage unavailable") };
        var grain = Grain(state);
        Assert.That(async () => await grain.RegisterAsync(Registration()), Throws.TypeOf<IOException>());
        Assert.That((await grain.GetAsync()).Descriptors, Is.Empty);
    }

    [Test]
    public void Invalid_hops_and_unknown_host_ids_are_rejected()
    {
        var registration = Registration();
        foreach (var hop in new LatticeSchemaRuntimeUpcaster[]
        {
            new() { SchemaId = 42, FromVersion = 2, ToVersion = 1, Transform = LatticeValueTransform.Passthrough() },
            new() { SchemaId = 42, FromVersion = 1, ToVersion = 3, Transform = LatticeValueTransform.Passthrough() },
            new() { SchemaId = 42, FromVersion = 1, ToVersion = 2, TransformId = "Some.Clr.Type" },
            new() { SchemaId = 42, FromVersion = 1, ToVersion = 2 },
            new() { SchemaId = 42, FromVersion = 1, ToVersion = 2, Transform = new() { Kind = (LatticeValueTransformKind)255 } },
            new() { SchemaId = 42, FromVersion = 1, ToVersion = 2, Transform = LatticeValueTransform.Passthrough(LatticeValueTransform.Const(LatticeConstant.Null())) },
        })
        {
            Assert.That(() => SchemaRuntimeCatalog.Merge(new(), registration with { Upcasters = [hop] }, Transforms),
                Throws.ArgumentException);
        }
    }

    [Test]
    public void Excessive_catalog_and_ambiguous_transform_are_rejected()
    {
        Assert.That(() => SchemaRuntimeCatalog.Merge(new(), new()
        {
            Descriptors = Enumerable.Range(1, 129).Select(v => new LatticeSchemaDescriptor(1, (uint)v, "version")).ToArray(),
        }, Transforms), Throws.ArgumentException);
        var registration = Registration();
        Assert.That(() => SchemaRuntimeCatalog.Merge(new(), registration with
        {
            Upcasters = [registration.Upcasters[0] with { TransformId = "also-code" }],
        }, Transforms), Throws.ArgumentException);
    }
}
