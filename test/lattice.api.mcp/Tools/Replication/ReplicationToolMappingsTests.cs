namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationToolMappings.ToMergeMode(string?)"/> - the
/// point where a wire-supplied merge-mode string is reconstructed into its
/// <see cref="LatticeMergeMode"/> before replication is enabled for a tree.
/// </summary>
/// <remarks>
/// The documented contract is that a <c>null</c>, empty, or unrecognised value is
/// rejected rather than defaulted. Two gaps let a malformed value slip past a bare
/// <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/> plus
/// <see cref="Enum.IsDefined{TEnum}(TEnum)"/> guard: an in-range numeric ordinal
/// (<c>"11"</c> binds to the defined value <see cref="LatticeMergeMode.RwSet"/>),
/// and a comma-combined name, which <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/>
/// treats as a bitwise OR (<c>"OrSet,GSet"</c> = 1|10 = 11 folds onto the distinct
/// defined member <see cref="LatticeMergeMode.RwSet"/>). Either would enable a tree
/// under a merge mode the caller never named. These tests pin the round-trip
/// name-equality guard that closes both, while confirming a genuine name in any
/// case still resolves.
/// </remarks>
[TestFixture]
public sealed class ReplicationToolMappingsTests
{
    [TestCase("LwwRegister", LatticeMergeMode.LwwRegister)]
    [TestCase("OrSet", LatticeMergeMode.OrSet)]
    [TestCase("orset", LatticeMergeMode.OrSet)]
    [TestCase("PNCOUNTER", LatticeMergeMode.PnCounter)]
    [TestCase("RwSet", LatticeMergeMode.RwSet)]
    [TestCase("gset", LatticeMergeMode.GSet)]
    public void ToMergeMode_parses_a_defined_mode_name_case_insensitively(string mode, LatticeMergeMode expected)
        => Assert.That(ReplicationToolMappings.ToMergeMode(mode), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    public void ToMergeMode_rejects_an_absent_mode(string? mode)
        => Assert.That(
            () => ReplicationToolMappings.ToMergeMode(mode),
            Throws.ArgumentException,
            "A merge mode is required to enable replication; a null or empty value must be rejected, not defaulted.");

    [Test]
    public void ToMergeMode_rejects_an_unrecognised_name()
        => Assert.That(
            () => ReplicationToolMappings.ToMergeMode("Sideways"),
            Throws.ArgumentException.With.Message.Contains("Sideways"));

    [TestCase("99")]
    [TestCase("-1")]
    [TestCase("2147483647")]
    public void ToMergeMode_rejects_an_out_of_range_numeric_ordinal(string mode)
        => Assert.That(
            () => ReplicationToolMappings.ToMergeMode(mode),
            Throws.ArgumentException,
            "A numeric string that is not a defined LatticeMergeMode must be rejected, not bound to (LatticeMergeMode)n.");

    [TestCase("11")] // RwSet - a DEFINED ordinal, so an Enum.IsDefined guard alone accepts it.
    [TestCase("3")]  // VersionVector - a DEFINED ordinal.
    [TestCase("0")]  // LwwRegister - a DEFINED ordinal.
    public void ToMergeMode_rejects_an_in_range_numeric_ordinal(string mode)
        => Assert.That(
            () => ReplicationToolMappings.ToMergeMode(mode),
            Throws.ArgumentException,
            "A numeric string equal to a defined ordinal is still not a merge-mode NAME; Enum.IsDefined accepts it, "
            + "so only a round-trip name check rejects it before a tree is enabled under a mode the caller never named.");

    [TestCase("OrSet,GSet")]     // 1|10 = 11 = RwSet, a DISTINCT defined member.
    [TestCase("OrSet,PnCounter")] // 1|2 = 3 = VersionVector, a DISTINCT defined member.
    public void ToMergeMode_rejects_a_comma_combined_name(string mode)
        => Assert.That(
            () => ReplicationToolMappings.ToMergeMode(mode),
            Throws.ArgumentException,
            "Enum.TryParse treats a comma list as a bitwise OR, so 'OrSet,GSet' folds onto the distinct defined "
            + "member RwSet and slips past Enum.IsDefined; the round-trip name check is what rejects it.");
}
