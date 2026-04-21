using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for audit hygiene fixes that live in the Orleans.Lattice
/// assembly but do not fit inside any single grain's unit-test file.
/// </summary>
[TestFixture]
public class AuditHygieneRegressionTests
{
    /// <summary>
    /// Regression: every grain implementation must use
    /// <c>ILogger&lt;TSelf&gt;</c> (not plain <c>ILogger</c>) so that per-category
    /// filter configuration works uniformly across the assembly. Fails if
    /// any grain-class constructor declares a non-generic <c>ILogger</c>
    /// parameter or field.
    /// </summary>
    [Test]
    public void Every_grain_uses_generic_ILogger_category()
    {
        var assembly = typeof(LatticeOptions).Assembly;

        var grainTypes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract
                        && typeof(IGrainBase).IsAssignableFrom(t))
            .ToList();

        var offenders = new List<string>();
        foreach (var grainType in grainTypes)
        {
            var ctors = grainType.GetConstructors(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var ctor in ctors)
            {
                foreach (var p in ctor.GetParameters())
                {
                    if (p.ParameterType == typeof(Microsoft.Extensions.Logging.ILogger))
                        offenders.Add($"{grainType.Name}.ctor({p.Name}): non-generic ILogger");
                }
            }

            var fields = grainType.GetFields(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var f in fields)
            {
                if (f.FieldType == typeof(Microsoft.Extensions.Logging.ILogger))
                    offenders.Add($"{grainType.Name}.{f.Name}: non-generic ILogger");
            }
        }

        Assert.That(offenders, Is.Empty,
            "All grain loggers must be ILogger<TSelf> for consistent category filtering.\n"
            + string.Join("\n", offenders));
    }

    /// <summary>
    /// Regression: the public telemetry meter name must remain
    /// <c>orleans.lattice</c> (the Orleans meter convention). Before the
    /// fix, internal telemetry hooks mixed <c>lattice.*</c> and
    /// <c>orleans.lattice.*</c> prefixes; locking the constant here prevents
    /// instruments from being published under the wrong namespace.
    /// </summary>
    [Test]
    public void LatticeMetrics_meter_name_is_orleans_lattice()
    {
        Assert.That(LatticeMetrics.MeterName, Is.EqualTo("orleans.lattice"));
    }
}
