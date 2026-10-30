using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Detectors for the facade's target check: while a tree's schema catalog is
/// non-empty, <see cref="LatticeSchemaControl"/> sets or advances the version
/// target only to a registered version, in the same family, not below the current
/// target, that the current target reaches through registered hops. Each refusal
/// must happen before the version admin is reached. The catalog is the host's
/// declarations - family 7, versions 1 to 3, one hop from 1 to 3 - merged with an
/// empty runtime registration, through a real <see cref="SchemaRuntimeRegistry"/>.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaControlVersionTargetTests
{
    private const string Tree = "orders";
    private const uint Family = 7;

    private static (LatticeSchemaControl Control, ILatticeSchemaVersionAdmin Admin) Create(
        LatticeSchemaVersionConfig? current, bool declared = true)
    {
        var admin = Substitute.For<ILatticeSchemaVersionAdmin>();
        admin.GetVersionConfigAsync(Tree, Arg.Any<CancellationToken>()).Returns(current);
        admin.AdvanceTargetVersionAsync(Tree, Arg.Any<uint>(), Arg.Any<CancellationToken>())
            .Returns(call => current!.Value with { TargetVersion = call.ArgAt<uint>(1) });

        var transforms = new LatticeValueTransformRegistry([]);
        var builder = new LatticeSchemaRegistryBuilder();
        if (declared)
        {
            builder.AddSchema(Family, 1, "orders-v1")
                .AddSchema(Family, 2, "orders-v2")
                .AddSchema(Family, 3, "orders-v3")
                .AddUpcaster(Family, 1, 3, LatticeValueTransform.Passthrough());
        }

        var grain = Substitute.For<ILatticeSchemaRuntimeRegistryGrain>();
        grain.GetAsync().Returns(new LatticeSchemaRuntimeRegistration());
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<ILatticeSchemaRuntimeRegistryGrain>(Tree, Arg.Any<string?>()).Returns(grain);
        var runtime = new SchemaRuntimeRegistry(grains, builder.Build(transforms), transforms, []);

        var control = new LatticeSchemaControl(
            Substitute.For<ILatticeSchemaAdmin>(),
            Substitute.For<ILatticeSchemaRemediationAdmin>(),
            Substitute.For<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(RecordingAccessGate.Allow()),
            Options.Create(new LatticeApiSchemaOptions()),
            new ServiceCollection().AddSingleton(admin).AddSingleton(runtime).BuildServiceProvider(),
            new DefaultTenantContextResolver());
        return (control, admin);
    }

    private static void AssertNeverSet(ILatticeSchemaVersionAdmin admin) =>
        admin.DidNotReceive().SetVersionConfigAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());

    [Test]
    public void SetVersionConfig_refuses_a_target_with_no_registered_descriptor()
    {
        var (control, admin) = Create(current: null);

        Assert.That(async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(Family, 4)),
            Throws.InvalidOperationException);
        AssertNeverSet(admin);
    }

    [Test]
    public void SetVersionConfig_refuses_a_lower_target_or_another_family()
    {
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 3));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(Family, 1)),
                Throws.InvalidOperationException, "a lower target");
            Assert.That(async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(Family + 1, 3)),
                Throws.InvalidOperationException, "another family");
        });
        AssertNeverSet(admin);
    }

    [Test]
    public void SetVersionConfig_refuses_a_target_the_current_target_cannot_reach()
    {
        // v2 is registered, but the only hop from v1 goes to v3 and overshoots it.
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 1));

        Assert.That(async () => await control.SetVersionConfigAsync(Tree, new LatticeSchemaVersionConfig(Family, 2)),
            Throws.InvalidOperationException);
        AssertNeverSet(admin);
    }

    [Test]
    public async Task SetVersionConfig_accepts_a_registered_target_the_current_target_reaches()
    {
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 1));
        var config = new LatticeSchemaVersionConfig(Family, 3);

        await control.SetVersionConfigAsync(Tree, config);

        await admin.Received(1).SetVersionConfigAsync(Tree, config, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetVersionConfig_with_an_empty_catalog_accepts_any_target()
    {
        // With nothing declared or registered the tree is in stamping-only mode:
        // the facade does not govern the target, so even a lower one is accepted.
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 3), declared: false);
        var config = new LatticeSchemaVersionConfig(Family, 1);

        await control.SetVersionConfigAsync(Tree, config);

        await admin.Received(1).SetVersionConfigAsync(Tree, config, Arg.Any<CancellationToken>());
    }

    [Test]
    public void AdvanceTargetVersion_refuses_a_target_with_no_registered_path()
    {
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 1));

        Assert.That(async () => await control.AdvanceTargetVersionAsync(Tree, 2), Throws.InvalidOperationException);
        admin.DidNotReceive().AdvanceTargetVersionAsync(Arg.Any<string>(), Arg.Any<uint>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdvanceTargetVersion_accepts_a_registered_reachable_target()
    {
        var (control, admin) = Create(new LatticeSchemaVersionConfig(Family, 1));

        var advanced = await control.AdvanceTargetVersionAsync(Tree, 3);

        Assert.That(advanced.TargetVersion, Is.EqualTo(3u));
        await admin.Received(1).AdvanceTargetVersionAsync(Tree, 3, Arg.Any<CancellationToken>());
    }
}
