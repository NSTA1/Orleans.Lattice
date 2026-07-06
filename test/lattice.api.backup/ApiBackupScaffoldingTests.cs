using Orleans.Lattice.Api.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Scaffolding sanity check for the <c>Orleans.Lattice.Api.Backup</c> package:
/// the assembly loads and the reserved control-API serialization-alias prefix is
/// stable, guarding the reservation later backup control-API releases depend on.
/// </summary>
public sealed class ApiBackupScaffoldingTests
{
    [Test]
    public void Alias_prefix_is_the_reserved_backup_control_api_namespace()
    {
        Assert.That(ApiBackupTypeAliases.AliasPrefix, Is.EqualTo("oib."));
    }
}
