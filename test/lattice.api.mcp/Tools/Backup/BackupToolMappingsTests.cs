using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the enum-parsing surface of <see cref="BackupToolMappings"/> -
/// the point where a wire-supplied <c>scopeKind</c> or restore <c>mode</c> string
/// is reconstructed into its domain enum before a backup or restore is dispatched.
/// </summary>
/// <remarks>
/// The documented contract is that an unrecognised value is rejected with an
/// <see cref="ArgumentException"/> naming the parameter. A numeric string is the
/// gap that regressed: <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/>
/// happily binds <c>"99"</c> to the undefined enum value <c>(TEnum)99</c> and
/// returns <c>true</c>, so without an <see cref="Enum.IsDefined{TEnum}(TEnum)"/>
/// guard an out-of-range ordinal slipped past validation and reached the facade as
/// a garbage mode/scope. These tests pin the guard: a defined name still parses, an
/// unknown name is rejected, and an out-of-range numeric ordinal is rejected too.
/// </remarks>
[TestFixture]
public sealed class BackupToolMappingsTests
{
    // ---- restore mode ------------------------------------------------------

    [TestCase("InPlace", LatticeRestoreMode.InPlace)]
    [TestCase("inplace", LatticeRestoreMode.InPlace)]
    [TestCase("ShadowCutover", LatticeRestoreMode.ShadowCutover)]
    [TestCase("shadowcutover", LatticeRestoreMode.ShadowCutover)]
    public void ToRestoreMode_parses_a_defined_mode_name_case_insensitively(string mode, LatticeRestoreMode expected)
        => Assert.That(BackupToolMappings.ToRestoreMode(mode), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    public void ToRestoreMode_defaults_to_in_place_when_absent(string? mode)
        => Assert.That(BackupToolMappings.ToRestoreMode(mode), Is.EqualTo(LatticeRestoreMode.InPlace));

    [Test]
    public void ToRestoreMode_rejects_an_unrecognised_name()
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode("Sideways"),
            Throws.ArgumentException.With.Message.Contains("Sideways"));

    [TestCase("99")]
    [TestCase("-1")]
    [TestCase("2147483647")]
    public void ToRestoreMode_rejects_an_out_of_range_numeric_ordinal(string mode)
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode(mode),
            Throws.ArgumentException,
            "A numeric string that is not a defined LatticeRestoreMode must be rejected, not bound to (LatticeRestoreMode)n.");

    // ---- scope kind --------------------------------------------------------

    [TestCase(null)]
    [TestCase("")]
    public void ToScope_defaults_to_whole_tree_when_kind_absent(string? kind)
    {
        var scope = BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null);

        Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.WholeTree));
    }

    [Test]
    public void ToScope_parses_a_defined_kind_name()
    {
        var scope = BackupToolMappings.ToScope("orders", "Prefix", keyOrPrefix: "p");

        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.Prefix));
            Assert.That(scope.KeyOrPrefix, Is.EqualTo("p"));
        });
    }

    [Test]
    public void ToScope_rejects_an_unrecognised_kind_name()
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", "Everything", keyOrPrefix: null),
            Throws.ArgumentException.With.Message.Contains("Everything"));

    [TestCase("99")]
    [TestCase("-1")]
    [TestCase("2147483647")]
    public void ToScope_rejects_an_out_of_range_numeric_ordinal(string kind)
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null),
            Throws.ArgumentException,
            "A numeric string that is not a defined BackupScopeKind must be rejected, not silently treated as WholeTree.");
}
