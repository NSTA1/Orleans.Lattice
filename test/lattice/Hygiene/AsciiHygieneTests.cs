using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>The core CI-selected gate owns the complete tracked text population.</summary>
[TestFixture]
public sealed class AsciiHygieneTests : AsciiHygieneTestsBase
{
    /// <inheritdoc />
    protected override string AllowlistRelativePath => "test/lattice/Hygiene/UnicodeAllowlist.tsv";
}
