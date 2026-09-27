using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppCapabilityCeilingTests
{
    [Test]
    public void Default_grants_no_operations_or_exceptions()
    {
        var ceiling = new AppCapabilityCeiling();
        Assert.That(ceiling.AllowedOperations, Is.EqualTo(LatticeOperation.None));
        Assert.That(ceiling.ApprovedExceptionScopes, Is.Empty);
    }

    [TestCase(LatticeOperation.None)]
    [TestCase(LatticeOperation.Read | LatticeOperation.Write)]
    public void Structural_preserves_mask_without_exception_consent(LatticeOperation operations)
    {
        var ceiling = AppCapabilityCeiling.Structural(operations);
        Assert.That(ceiling.AllowedOperations, Is.EqualTo(operations));
        Assert.That(ceiling.ApprovedExceptionScopes, Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Orleans_roundtrip_preserves_mask_and_explicit_exception_scopes(bool exceptions)
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppCapabilityCeiling).Assembly)
                .AddAssembly(typeof(LatticeScope).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var source = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.RangeRead);
        if (exceptions)
            source = source with
            {
                ApprovedExceptionScopes = new List<LatticeScope>
                {
                    LatticeScope.Tree("repo-context-memory"),
                    LatticeScope.Prefix("a/other-app/records", "shared/"),
                    LatticeScope.Key("legacy-tree", "key"),
                },
            };
        var copy = serializer.Deserialize<AppCapabilityCeiling>(serializer.SerializeToArray(source));
        Assert.That(copy.AllowedOperations, Is.EqualTo(source.AllowedOperations));
        Assert.That(copy.ApprovedExceptionScopes, Is.EqualTo(source.ApprovedExceptionScopes));
    }
}
