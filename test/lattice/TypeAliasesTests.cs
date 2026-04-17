using System.Reflection;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

public class TypeAliasesTests
{
    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string));

        foreach (var field in fields)
        {
            var value = (string)field.GetValue(null)!;
            Assert.That(value.Length, Is.LessThanOrEqualTo(6),
                $"TypeAliases.{field.Name} = \"{value}\" exceeds 6-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_ol_prefix()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string));

        foreach (var field in fields)
        {
            var value = (string)field.GetValue(null)!;
            Assert.That(value, Does.StartWith("ol."),
                $"TypeAliases.{field.Name} = \"{value}\" does not start with \"ol.\"");
        }
    }

    [Test]
    public void All_aliases_are_unique()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .ToList();

        var values = fields.Select(f => (string)f.GetValue(null)!).ToList();
        var duplicates = values
            .GroupBy(v => v)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty,
            $"Duplicate aliases found: {string.Join(", ", duplicates)}");
    }
}