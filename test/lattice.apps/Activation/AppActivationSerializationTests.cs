using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Orleans wire-format coverage for the activation pipeline's serializable types, plus the small
/// public helpers on them.
/// </summary>
[TestFixture]
public sealed class AppActivationSerializationTests
{
    private static Serializer CreateSerializer(out ServiceProvider services)
    {
        services = new ServiceCollection()
            .AddSerializer(builder => builder
                .AddAssembly(typeof(AppActivationOutcome).Assembly)
                .AddAssembly(typeof(LatticeScope).Assembly)
                .AddAssembly(typeof(TenantId).Assembly))
            .BuildServiceProvider();
        return services.GetRequiredService<Serializer>();
    }

    private static AppActivationOutcome Outcome(AppActivationFailure failure = AppActivationFailure.CeilingExceeded) => new()
    {
        Tenant = TenantId.Parse("acme"),
        Slug = AppSlug.Parse("notes"),
        Operation = AppActivationOperation.Reconcile,
        Failure = failure,
        Version = AppVersion.Parse("2.0.0"),
        State = AppRegistryLifecycleState.Enabled,
        Changed = true,
        Diagnostics = new[] { new AppManifestError("ceiling-scope", "$.roles[reader].scopes", "denied") },
        CompletedAtUtc = AppRegistryTestData.Start,
    };

    [Test]
    public void AppActivationOutcome_roundtrip_preserves_every_field()
    {
        var serializer = CreateSerializer(out var services);
        using var _ = services;
        var source = Outcome();

        var copy = serializer.Deserialize<AppActivationOutcome>(serializer.SerializeToArray(source));

        Assert.That(copy.Tenant, Is.EqualTo(source.Tenant));
        Assert.That(copy.Slug, Is.EqualTo(source.Slug));
        Assert.That(copy.Operation, Is.EqualTo(source.Operation));
        Assert.That(copy.Failure, Is.EqualTo(source.Failure));
        Assert.That(copy.Version, Is.EqualTo(source.Version));
        Assert.That(copy.State, Is.EqualTo(source.State));
        Assert.That(copy.Changed, Is.True);
        Assert.That(copy.Diagnostics, Is.EqualTo(source.Diagnostics));
        Assert.That(copy.CompletedAtUtc, Is.EqualTo(source.CompletedAtUtc));
        Assert.That(copy.Succeeded, Is.False);
    }

    [Test]
    public void AppActivationStatus_roundtrip_preserves_the_applied_manifest()
    {
        var serializer = CreateSerializer(out var services);
        using var _ = services;
        var source = new AppActivationStatus
        {
            Tenant = TenantId.Parse("acme"),
            Slug = AppSlug.Parse("notes"),
            LastOutcome = Outcome(AppActivationFailure.None),
            AppliedManifest = ActivationHarness.Manifest(trees: new[] { ActivationHarness.Tree("records", virtualShards: 16) }),
        };

        var copy = serializer.Deserialize<AppActivationStatus>(serializer.SerializeToArray(source));

        Assert.That(copy.Tenant, Is.EqualTo(source.Tenant));
        Assert.That(copy.Slug, Is.EqualTo(source.Slug));
        Assert.That(copy.LastOutcome.Succeeded, Is.True);
        Assert.That(copy.AppliedManifest!.Identity, Is.EqualTo(source.AppliedManifest.Identity));
        Assert.That(copy.AppliedManifest.Trees.Single().VirtualShardCount, Is.EqualTo(16));
    }

    [Test]
    public void Status_without_an_applied_manifest_roundtrips_as_null()
    {
        var serializer = CreateSerializer(out var services);
        using var _ = services;
        var source = new AppActivationStatus { Tenant = TenantId.Default, Slug = AppSlug.Parse("notes"), LastOutcome = Outcome() };

        var copy = serializer.Deserialize<AppActivationStatus>(serializer.SerializeToArray(source));

        Assert.That(copy.AppliedManifest, Is.Null);
    }

    [Test]
    public void Outcome_defaults_to_success_with_no_diagnostics()
    {
        var outcome = new AppActivationOutcome { Tenant = TenantId.Default, Slug = AppSlug.Parse("notes"), Operation = AppActivationOperation.Enable };

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.Diagnostics, Is.Empty);
        Assert.That(outcome.Version, Is.Null);
        Assert.That(outcome.State, Is.Null);
    }

    [Test]
    public void Enum_values_are_pinned()
    {
        Assert.That((int)AppActivationOperation.Enable, Is.EqualTo(0));
        Assert.That((int)AppActivationOperation.Reconcile, Is.EqualTo(3));
        Assert.That(Enum.GetValues<AppActivationOperation>(), Has.Length.EqualTo(4));
        Assert.That((int)AppActivationFailure.None, Is.EqualTo(0));
        Assert.That((int)AppActivationFailure.MembershipNotRegistered, Is.EqualTo(4));
        Assert.That((int)AppActivationFailure.Faulted, Is.EqualTo(14));
        Assert.That(Enum.GetValues<AppActivationFailure>(), Has.Length.EqualTo(15));
    }

    [Test]
    public void Aliases_are_unique_and_prefixed()
    {
        var aliases = new[]
        {
            AppActivationTypeAliases.AppActivationOperation,
            AppActivationTypeAliases.AppActivationFailure,
            AppActivationTypeAliases.AppActivationOutcome,
            AppActivationTypeAliases.AppActivationStatus,
            AppActivationTypeAliases.IAppActivationGrain,
        };

        Assert.That(aliases, Is.Unique);
        Assert.That(aliases, Has.All.StartsWith("oap."));
    }

    [Test]
    public void Provisioner_entry_carries_the_declared_pins_only_when_declared()
    {
        Assert.That(LatticeAppTreeProvisioner.BuildEntry(ActivationHarness.Tree("records")), Is.Null);

        var entry = LatticeAppTreeProvisioner.BuildEntry(new AppTreeDeclaration
        {
            Name = "records",
            ShardCount = 2,
            VirtualShardCount = 16,
            MaxLeafKeys = 64,
            MaxInternalChildren = 8,
            WalPartitions = 4,
        })!;

        Assert.That(entry.ShardCount, Is.EqualTo(2));
        Assert.That(entry.MaxLeafKeys, Is.EqualTo(64));
        Assert.That(entry.MaxInternalChildren, Is.EqualTo(8));
        Assert.That(entry.WalPartitions, Is.EqualTo(4));
        Assert.That(entry.ShardMap!.Slots, Has.Length.EqualTo(16));
        Assert.That(LatticeAppTreeProvisioner.BuildEntry(ActivationHarness.Tree("records", virtualShards: 4096))!.ShardMap!.Slots, Has.Length.EqualTo(4096));

        // A virtual space smaller than the physical fan-out cannot route; provisioning fails and
        // the pipeline records it as a tree provisioning failure.
        Assert.Throws<ArgumentException>(() => LatticeAppTreeProvisioner.BuildEntry(new AppTreeDeclaration { Name = "records", ShardCount = 8, VirtualShardCount = 4 }));
    }
}
