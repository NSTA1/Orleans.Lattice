using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    [Test]
    public void Manifest_Orleans_roundtrip_preserves_identity_and_nested_declarations()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppManifest).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var source = Manifest;
        var copy = serializer.Deserialize<AppManifest>(serializer.SerializeToArray(source));
        Assert.That(AppManifestValidator.Validate(copy).IsValid, Is.True);
        Assert.That(copy.Identity, Is.EqualTo(source.Identity));
        Assert.That(copy.Trees, Is.EqualTo(source.Trees));
        Assert.That(copy.Roles[0].Name, Is.EqualTo(source.Roles[0].Name));
        Assert.That(copy.Roles[0].Operations, Is.EqualTo(source.Roles[0].Operations));
        Assert.That(copy.Roles[0].Scopes, Is.EqualTo(source.Roles[0].Scopes));
        Assert.That(copy.Replication, Is.EqualTo(source.Replication));
        Assert.That(copy.Schema, Is.EqualTo(source.Schema));
        Assert.That(copy.Subscriptions, Is.EqualTo(source.Subscriptions));
        Assert.That(copy.McpTools, Is.EqualTo(source.McpTools));
        var error = new AppManifestError("code", "$.trees", "diagnostic");
        Assert.That(serializer.Deserialize<AppManifestError>(serializer.SerializeToArray(error)), Is.EqualTo(error));
    }

    [Test]
    public void AppsTypeAliases_every_constant_has_one_owner_and_no_dependency_collision()
    {
        var assembly = typeof(AppManifest).Assembly;
        var aliases = typeof(AppsTypeAliases).GetFields(BindingFlags.Static | BindingFlags.NonPublic)
            .Where(f => f.IsLiteral).Select(f => (string)f.GetRawConstantValue()!).ToArray();
        var owners = assembly.GetTypes().Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null).ToArray();
        Assert.That(aliases, Has.Length.EqualTo(15));
        Assert.That(aliases.Distinct().Count(), Is.EqualTo(aliases.Length));
        foreach (var alias in aliases)
        {
            Assert.That(alias, Does.StartWith("oap."));
            Assert.That(alias.Length, Is.LessThanOrEqualTo(6));
            Assert.That(owners.Count(t => t.GetCustomAttribute<AliasAttribute>()?.Alias == alias), Is.EqualTo(1));
        }
        var dependencies = new[] { typeof(ILattice).Assembly, typeof(Auth.LatticeScope).Assembly, typeof(Replication.LatticeReplicationOptions).Assembly };
        foreach (var dependency in dependencies)
            Assert.That(dependency.GetTypes().Select(t => t.GetCustomAttribute<AliasAttribute>()?.Alias)
                .Where(a => a is not null).Intersect(aliases), Is.Empty);
    }
}
