using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Audits the same-silo deep-copy contract for every exception the abstractions
/// package declares. Orleans serialises a grain result cross-silo but
/// <em>deep-copies</em> it when the callee is co-located, and the copier generated
/// for a <c>[GenerateSerializer]</c> exception deriving from a BCL exception
/// <em>subclass</em> asks the runtime for a base-type copier Orleans does not
/// register - so the copy fails with an opaque <c>KeyNotFoundException</c> that
/// masks the real fault.
/// <para>
/// The package's convention is that every contract exception derives directly from
/// <see cref="Exception"/>, which satisfies the contract by construction whether or
/// not the type is serializable today. The first test enforces that convention over
/// the whole exception population, so it is never vacuous; the second runs the real
/// copier over whichever of them carry <c>[GenerateSerializer]</c>, so it is armed
/// the moment one does.
/// </para>
/// </summary>
[TestFixture]
public sealed class AbstractionsExceptionDeepCopyContractTests
{
    private static readonly Assembly AbstractionsAssembly = typeof(ILatticeStateQuery).Assembly;

    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Every_exception_declared_in_the_package_derives_directly_from_system_exception()
    {
        var exceptions = ExceptionTypes();

        Assert.That(exceptions, Is.Not.Empty,
            "No exception types were discovered in the abstractions assembly; the guard would be inert.");

        var offenders = exceptions
            .Where(t => t.BaseType != typeof(Exception))
            .Select(t => $"{t.FullName} derives from {t.BaseType?.FullName}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(offenders, Is.Empty,
            "Every contract exception must derive directly from System.Exception, so marking it "
            + "[GenerateSerializer] can never break a same-silo (co-located) grain-result deep copy. "
            + "A type that must derive from a BCL exception subclass needs a no-op [RegisterCopier] "
            + "IDeepCopier<T> next to it. Offenders: " + string.Join("; ", offenders));
    }

    [Test]
    public void Every_serializable_exception_deep_copies_on_a_same_silo_boundary()
    {
        var offenders = new List<string>();

        foreach (var type in ExceptionTypes().Where(HasGenerateSerializer))
        {
            var closedCopierType = typeof(DeepCopier<>).MakeGenericType(type);
            var copier = _services.GetService(closedCopierType);
            if (copier is null)
            {
                offenders.Add($"{type.FullName}: no {closedCopierType.Name} registered");
                continue;
            }

            var copyMethod = closedCopierType.GetMethod("Copy", [type]);
            if (copyMethod is null)
            {
                offenders.Add($"{type.FullName}: {closedCopierType.Name} has no single-argument Copy method");
                continue;
            }

            var instance = RuntimeHelpers.GetUninitializedObject(type);

            try
            {
                var copy = copyMethod.Invoke(copier, [instance]);
                if (copy is null)
                {
                    offenders.Add($"{type.FullName}: same-silo deep copy returned null");
                }
                else if (!type.IsInstanceOfType(copy))
                {
                    offenders.Add($"{type.FullName}: same-silo deep copy returned {copy.GetType().FullName}");
                }
            }
            catch (Exception ex)
            {
                var inner = ex is TargetInvocationException { InnerException: { } cause } ? cause : ex;
                offenders.Add($"{type.FullName}: {inner.GetType().Name}: {inner.Message}");
            }
        }

        Assert.That(offenders, Is.Empty,
            "Every [GenerateSerializer] exception must deep-copy across a same-silo (co-located) grain "
            + "boundary. Offenders:" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    private static bool HasGenerateSerializer(Type type) =>
        type.GetCustomAttributes(inherit: false)
            .Any(a => a.GetType().Name == "GenerateSerializerAttribute");

    private static IReadOnlyList<Type> ExceptionTypes() =>
        AbstractionsAssembly.GetTypes()
            .Where(t => typeof(Exception).IsAssignableFrom(t)
                && !t.IsAbstract
                && !t.ContainsGenericParameters)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();
}
