using System.Reflection;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Guards the invariants of this package's own Orleans serialization alias
/// table, <see cref="TypeAliases"/>. The core
/// <c>TypeAliasesTests.Every_alias_constant_is_referenced_by_exactly_one_type</c>
/// gate is scoped to the core assembly, so a package that declares its own
/// table needs its own gate; this fixture is that gate, and it stays load
/// bearing as later work adds alias constants.
/// </summary>
[TestFixture]
public sealed class TypeAliasesTests
{
    /// <summary>
    /// The alias constants declared by <see cref="TypeAliases"/>, excluding the
    /// two constants that describe the table's own conventions rather than
    /// naming a type.
    /// </summary>
    private static IReadOnlyList<FieldInfo> AliasConstants() =>
        typeof(TypeAliases)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral
                && f.FieldType == typeof(string)
                && f.Name != nameof(TypeAliases.Prefix))
            .OrderBy(f => f.Name, StringComparer.Ordinal)
            .ToArray();

    [Test]
    public void Alias_table_is_a_non_instantiable_static_class()
    {
        var table = typeof(TypeAliases);

        Assert.Multiple(() =>
        {
            Assert.That(table.IsAbstract && table.IsSealed, Is.True,
                $"{table.FullName} must be a static class so it can only ever hold constants.");
            Assert.That(table.IsPublic, Is.False,
                $"{table.FullName} must stay internal: it is a wire-format detail of this "
                + "package, not part of its public surface.");
        });
    }

    [Test]
    public void Alias_table_conventions_match_the_core_alias_table()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TypeAliases.Prefix, Is.EqualTo("ol."),
                "Every Orleans.Lattice alias table shares the canonical 'ol.' prefix so the "
                + "cluster-wide alias registry stays one recognisable namespace.");
            Assert.That(TypeAliases.MaxAliasLength, Is.EqualTo(6),
                "Aliases are capped at six characters because every serialized payload "
                + "carries one; the cap matches the core table's.");
        });
    }

    [Test]
    public void Every_alias_constant_uses_the_canonical_prefix_and_length()
    {
        var offenders = AliasConstants()
            .Select(f => new { f.Name, Value = (string)f.GetRawConstantValue()! })
            .Where(a => !a.Value.StartsWith(TypeAliases.Prefix, StringComparison.Ordinal)
                || a.Value.Length > TypeAliases.MaxAliasLength)
            .Select(a => $"{a.Name} = '{a.Value}'")
            .ToArray();

        Assert.That(offenders, Is.Empty,
            $"Every alias constant must start with '{TypeAliases.Prefix}' and be at most "
            + $"{TypeAliases.MaxAliasLength} characters. Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Every_alias_constant_is_unique_within_the_table()
    {
        var duplicates = AliasConstants()
            .GroupBy(f => (string)f.GetRawConstantValue()!, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => $"'{g.Key}' declared by {string.Join(", ", g.Select(f => f.Name))}")
            .ToArray();

        Assert.That(duplicates, Is.Empty,
            "An alias value identifies exactly one type on the wire, so no two constants may "
            + "share one. Duplicates: " + string.Join("; ", duplicates));
    }
}
