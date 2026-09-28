using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Orleans wire-format coverage for the app-registry types and their stable alias table.
/// </summary>
[TestFixture]
public sealed class AppRegistrySerializationTests
{
    private static Serializer CreateSerializer(out ServiceProvider services)
    {
        services = new ServiceCollection()
            .AddSerializer(builder => builder
                .AddAssembly(typeof(AppRegistryRecord).Assembly)
                .AddAssembly(typeof(LatticeScope).Assembly)
                .AddAssembly(typeof(TenantId).Assembly))
            .BuildServiceProvider();
        return services.GetRequiredService<Serializer>();
    }

    [Test]
    public void AppRegistryRecord_roundtrip_preserves_every_field()
    {
        var serializer = CreateSerializer(out var services);
        using var _ = services;
        var source = new AppRegistryRecord
        {
            Isolation = new AppIsolationContext { Tenant = TenantId.Parse("acme"), ClusterId = "cluster-a" },
            Slug = AppSlug.Parse("notes"),
            Version = AppVersion.Parse("2.1.0-rc.1+build.5"),
            Provenance = new AppProvenance { Source = "in-image", Publisher = "contoso", Reference = "notes.json" },
            Ceiling = new AppCapabilityCeiling
            {
                AllowedOperations = LatticeOperation.Read | LatticeOperation.Write,
                ApprovedExceptionScopes = new[] { LatticeScope.Prefix("a/other/records", "shared/") },
            },
            CeilingVersion = AppVersion.Parse("2.1.0-rc.1+build.5"),
            RoleBindings = new[] { AppRoleBinding.Create("reader", "readers"), AppRoleBinding.Create("writer", "writers") },
            State = AppRegistryLifecycleState.Disabled,
            Revision = 9,
            InstalledAtUtc = AppRegistryTestData.Start,
            StateChangedAtUtc = AppRegistryTestData.Start.AddHours(1),
            ConsentedAtUtc = AppRegistryTestData.Start.AddMinutes(30),
            ConsentedBy = "operator",
        };

        var copy = serializer.Deserialize<AppRegistryRecord>(serializer.SerializeToArray(source));

        Assert.That(copy.Isolation, Is.EqualTo(source.Isolation));
        Assert.That(copy.Slug, Is.EqualTo(source.Slug));
        Assert.That(copy.Version, Is.EqualTo(source.Version));
        Assert.That(copy.Provenance, Is.EqualTo(source.Provenance));
        Assert.That(copy.Ceiling.AllowedOperations, Is.EqualTo(source.Ceiling.AllowedOperations));
        Assert.That(copy.Ceiling.ApprovedExceptionScopes, Is.EqualTo(source.Ceiling.ApprovedExceptionScopes));
        Assert.That(copy.CeilingVersion, Is.EqualTo(source.CeilingVersion));
        Assert.That(copy.RoleBindings, Is.EqualTo(source.RoleBindings));
        Assert.That(copy.State, Is.EqualTo(source.State));
        Assert.That(copy.Revision, Is.EqualTo(source.Revision));
        Assert.That(copy.InstalledAtUtc, Is.EqualTo(source.InstalledAtUtc));
        Assert.That(copy.StateChangedAtUtc, Is.EqualTo(source.StateChangedAtUtc));
        Assert.That(copy.ConsentedAtUtc, Is.EqualTo(source.ConsentedAtUtc));
        Assert.That(copy.ConsentedBy, Is.EqualTo(source.ConsentedBy));
        Assert.That(copy.IsCeilingPinnedToVersion, Is.True);
    }

    [Test]
    public void Request_and_result_roundtrip()
    {
        var serializer = CreateSerializer(out var services);
        using var _ = services;
        var request = AppRegistryTestData.Request(tenant: TenantId.Parse("acme")) with { ExpectedVersion = AppRegistryTestData.V1 };
        var result = AppRegistryTransitionResult.Rejected(
            AppRegistryTestData.Record(AppRegistryLifecycleState.Installed), AppRegistryTransitionError.InvalidTransition, "nope");

        var requestCopy = serializer.Deserialize<AppRegistryInstallRequest>(serializer.SerializeToArray(request));
        var resultCopy = serializer.Deserialize<AppRegistryTransitionResult>(serializer.SerializeToArray(result));

        Assert.That(requestCopy.Tenant, Is.EqualTo(request.Tenant));
        Assert.That(requestCopy.ExpectedVersion, Is.EqualTo(AppRegistryTestData.V1));
        Assert.That(requestCopy.Identity, Is.EqualTo(request.Identity));
        Assert.That(requestCopy.RoleBindings, Is.EqualTo(request.RoleBindings));
        Assert.That(requestCopy.Ceiling.AllowedOperations, Is.EqualTo(request.Ceiling.AllowedOperations));
        Assert.That(resultCopy.Error, Is.EqualTo(AppRegistryTransitionError.InvalidTransition));
        Assert.That(resultCopy.Message, Is.EqualTo("nope"));
        Assert.That(resultCopy.Succeeded, Is.False);
        Assert.That(resultCopy.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public void AppRegistryTypeAliases_each_have_one_owner_and_collide_with_nothing()
    {
        var assembly = typeof(AppRegistryRecord).Assembly;
        static string[] Constants(Type table) => table.GetFields(BindingFlags.Static | BindingFlags.NonPublic)
            .Where(f => f.IsLiteral).Select(f => (string)f.GetRawConstantValue()!).ToArray();
        var aliases = Constants(typeof(AppRegistryTypeAliases));
        var owners = assembly.GetTypes().Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null).ToArray();

        Assert.That(aliases, Has.Length.EqualTo(8));
        Assert.That(aliases.Distinct().Count(), Is.EqualTo(aliases.Length));
        Assert.That(aliases.Intersect(Constants(typeof(AppsTypeAliases))), Is.Empty, "the two apps alias tables are disjoint");
        foreach (var alias in aliases)
        {
            Assert.That(alias, Does.StartWith("oap."));
            Assert.That(alias.Length, Is.LessThanOrEqualTo(6));
            Assert.That(owners.Count(t => t.GetCustomAttribute<AliasAttribute>()?.Alias == alias), Is.EqualTo(1), alias);
        }

        var dependencies = new[] { typeof(ILattice).Assembly, typeof(LatticeScope).Assembly, typeof(Replication.LatticeReplicationOptions).Assembly };
        foreach (var dependency in dependencies)
        {
            Assert.That(dependency.GetTypes().Select(t => t.GetCustomAttribute<AliasAttribute>()?.Alias)
                .Where(a => a is not null).Intersect(aliases), Is.Empty);
        }
    }

    [Test]
    public void Success_and_rejection_factories_set_the_outcome()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled);

        var success = AppRegistryTransitionResult.Success(record, changed: true);
        var rejection = AppRegistryTransitionResult.Rejected(null, AppRegistryTransitionError.NotInstalled, "missing");

        Assert.That(success.Succeeded && success.Changed, Is.True);
        Assert.That(success.Record, Is.SameAs(record));
        Assert.That(success.Message, Is.Null);
        Assert.That(rejection.Succeeded || rejection.Changed, Is.False);
        Assert.That(rejection.Error, Is.EqualTo(AppRegistryTransitionError.NotInstalled));
    }

    [Test]
    public void Record_defaults_and_tenant_shorthand()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Installed, tenant: TenantId.Parse("acme"));

        Assert.That(record.Tenant, Is.EqualTo(TenantId.Parse("acme")));
        Assert.That(new AppRegistryInstallRequest
        {
            Identity = new AppIdentity { Slug = AppRegistryTestData.Slug, Version = AppRegistryTestData.V1 },
            Ceiling = new AppCapabilityCeiling(),
        }.Tenant, Is.EqualTo(TenantId.Default), "an install defaults to the default tenant");
        Assert.That(new AppRegistryInstallRequest
        {
            Identity = new AppIdentity { Slug = AppRegistryTestData.Slug, Version = AppRegistryTestData.V1 },
            Ceiling = new AppCapabilityCeiling(),
        }.ExpectedVersion, Is.Null, "an install request applies against whatever is installed by default");
        Assert.That(record.RoleBindings, Is.Empty);
        Assert.That(AppRegistryTestData.Record(AppRegistryLifecycleState.Installed, version: AppRegistryTestData.V2, ceilingVersion: AppRegistryTestData.V1)
            .IsCeilingPinnedToVersion, Is.False);
    }
}
