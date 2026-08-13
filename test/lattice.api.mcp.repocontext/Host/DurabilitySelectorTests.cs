using System.Data.Common;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="DurabilitySelector"/>'s process-wide concerns: the
/// ADO.NET provider factories register idempotently and are resolvable by the
/// invariant names the grain-storage and reminder providers use.
/// </summary>
[TestFixture]
public sealed class DurabilitySelectorTests
{
    [Test]
    public void RegisterAdoNetFactories_registers_the_sqlite_and_postgres_invariants()
    {
        DurabilitySelector.RegisterAdoNetFactories();

        Assert.Multiple(() =>
        {
            Assert.That(
                DbProviderFactories.GetFactory(SqliteSchemaInitializer.InvariantName),
                Is.Not.Null);
            Assert.That(
                DbProviderFactories.GetFactory(DurabilitySelector.PostgresInvariantName),
                Is.Not.Null);
        });
    }

    [Test]
    public void RegisterAdoNetFactories_is_idempotent()
    {
        DurabilitySelector.RegisterAdoNetFactories();

        Assert.That(() => DurabilitySelector.RegisterAdoNetFactories(), Throws.Nothing);
    }

    [Test]
    public void ConfigureDurability_rejects_a_null_config()
        => Assert.That(
            () => DurabilitySelector.ConfigureDurability(null!, null!),
            Throws.ArgumentNullException);

    [Test]
    public void PostgresInvariantName_is_the_npgsql_factory_invariant()
        => Assert.That(DurabilitySelector.PostgresInvariantName, Is.EqualTo("Npgsql"));
}
