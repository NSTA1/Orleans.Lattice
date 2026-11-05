using System.Collections;
using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="HandOffMarkerRows.Copy"/> covers every property of the leaf marker and of
/// every type it holds (a5e2e2ef's S2 on #5866): the conditional close (E3) mutates a copy
/// of its open's row, so a property the copy forgets would be written back as its default.
/// Every property is filled by reflection with a distinct non-default value, so a property
/// added later is covered - or fails here - without this test naming it.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerRowsCopyTests
{
    private int _next = 1;

    [Test]
    public void The_copy_carries_every_property_of_the_marker_and_its_members_deeply()
    {
        var marker = (LeafHandOffState)Fill(typeof(LeafHandOffState));

        var copy = HandOffMarkerRows.Copy(marker);

        AssertDeepCopy(marker, copy, nameof(LeafHandOffState));
    }

    private object Fill(Type type)
    {
        var instance = Activator.CreateInstance(type)!;
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            Assert.That(property.CanWrite, Is.True, $"{type.Name}.{property.Name} cannot be set, so the copy cannot carry it");
            property.SetValue(instance, Sample(property.PropertyType, $"{type.Name}.{property.Name}"));
        }

        return instance;
    }

    private object Sample(Type type, string name)
    {
        var n = _next++;
        if (type == typeof(string))
        {
            return name + "-" + n;
        }

        if (type == typeof(Guid))
        {
            return new Guid(n, 0, 0, new byte[8]);
        }

        if (type == typeof(long))
        {
            return (long)n;
        }

        if (type == typeof(int))
        {
            return n;
        }

        if (type == typeof(bool))
        {
            return true;
        }

        if (type == typeof(long[]))
        {
            return new long[] { n, n + 1 };
        }

        if (type == typeof(bool[]))
        {
            return new[] { true, false };
        }

        if (Nullable.GetUnderlyingType(type) is { } underlying)
        {
            return Sample(underlying, name);
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var dictionary = (IDictionary)Activator.CreateInstance(type, StringComparer.Ordinal)!;
            var args = type.GetGenericArguments();
            dictionary[Sample(args[0], name + ".Key")] = Fill(args[1]);
            return dictionary;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            var list = (IList)Activator.CreateInstance(type)!;
            list.Add(Fill(type.GetGenericArguments()[0]));
            return list;
        }

        Assert.Fail($"{name} has type {type.Name}, which this test cannot fill: extend Sample and check Copy carries it.");
        return null!;
    }

    private static void AssertDeepCopy(object original, object copy, string path)
    {
        Assert.That(copy, Is.Not.SameAs(original), $"{path} was shared, not copied");
        foreach (var property in original.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var name = $"{path}.{property.Name}";
            var expected = property.GetValue(original);
            var actual = property.GetValue(copy);
            switch (expected)
            {
                case string or Guid or long or int or bool:
                    Assert.That(actual, Is.EqualTo(expected), $"{name} was not copied");
                    break;
                case long[] heads:
                    Assert.That(actual, Is.EqualTo(heads).And.Not.SameAs(heads), $"{name} was not copied, or was shared");
                    break;
                case bool[] bits:
                    Assert.That(actual, Is.EqualTo(bits).And.Not.SameAs(bits), $"{name} was not copied, or was shared");
                    break;
                case IDictionary dictionary:
                    var copied = (IDictionary)actual!;
                    Assert.That(copied, Is.Not.SameAs(dictionary), $"{name} was shared");
                    Assert.That(copied.Count, Is.EqualTo(dictionary.Count), $"{name} lost entries");
                    foreach (DictionaryEntry entry in dictionary)
                    {
                        Assert.That(copied.Contains(entry.Key), Is.True, $"{name}[{entry.Key}] was not copied");
                        AssertDeepCopy(entry.Value!, copied[entry.Key]!, $"{name}[{entry.Key}]");
                    }

                    break;
                case IList list:
                    var copiedList = (IList)actual!;
                    Assert.That(copiedList, Is.Not.SameAs(list), $"{name} was shared");
                    Assert.That(copiedList.Count, Is.EqualTo(list.Count), $"{name} lost entries");
                    for (var i = 0; i < list.Count; i++)
                    {
                        AssertDeepCopy(list[i]!, copiedList[i]!, $"{name}[{i}]");
                    }

                    break;
                default:
                    Assert.That(actual, Is.EqualTo(expected), $"{name} was not copied");
                    break;
            }
        }
    }
}
