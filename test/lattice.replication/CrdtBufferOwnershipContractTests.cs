using System.Reflection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Concrete buffer-ownership guard for the <c>Orleans.Lattice.Replication</c>
/// assembly, which declares one CRDT of its own:
/// <see cref="LatticeReplicationConfigEntry"/>.
/// <para>
/// The entry holds no <c>byte[]</c> directly - it composes an <see cref="RwFlag"/>
/// and an <see cref="MvRegister"/>, and its <c>Clone</c> and <c>MergeFrom</c>
/// delegate to theirs - so it inherits the contract rather than implementing it.
/// That is exactly why it is worth pinning: the entry was silently sharing the
/// mode register's value bytes for as long as <c>MvRegister.Clone</c> was shallow,
/// without a single line of its own being at fault. Registering it here means a
/// future composite in this package, or a regression in a primitive it composes,
/// fails <c>build-and-test</c> here too.
/// </para>
/// </summary>
[TestFixture]
public sealed class CrdtBufferOwnershipContractTests : CrdtBufferOwnershipContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeReplicationConfigEntry).Assembly;

    /// <inheritdoc />
    protected override Type CrdtInterfaceType => typeof(ICrdt<>);

    /// <inheritdoc />
    protected override IReadOnlyList<CrdtOwnershipSpecimen> Specimens { get; } =
    [
        new(
            typeof(LatticeReplicationConfigEntry),
            CreatePopulated: static () =>
            {
                var entry = new LatticeReplicationConfigEntry();
                entry.Enable("A", 1);
                entry.SetMode("A", LatticeMergeMode.OrSet);
                return entry;
            },
            CreateEmpty: static () => new LatticeReplicationConfigEntry(),
            Projections: []),
    ];
}
