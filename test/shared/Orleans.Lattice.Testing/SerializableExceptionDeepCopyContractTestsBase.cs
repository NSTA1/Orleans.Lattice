using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, library-agnostic guard proving every <c>[GenerateSerializer]</c>
/// exception in a package deep-copies across a <b>same-silo</b> (co-located)
/// grain boundary. Orleans serialises a grain result cross-silo but
/// <em>deep-copies</em> it when the callee is co-located with the caller. The
/// generated copier for a <c>[GenerateSerializer]</c> exception that derives from
/// a BCL exception <em>subclass</em> (for example <see cref="InvalidOperationException"/>,
/// <see cref="TimeoutException"/>, or <see cref="UnauthorizedAccessException"/>)
/// asks the runtime for a copier for that base type, which Orleans does not
/// register - so the same-silo copy throws an opaque
/// <c>KeyNotFoundException</c> ("Could not find a base type copier for ...") and
/// masks the real, actionable fault. The fix is a no-op
/// <c>[RegisterCopier] IDeepCopier&lt;T&gt;</c> next to the type (an exception is
/// immutable once constructed, so sharing the instance is a correct deep copy).
/// <para>
/// The discovery and invocation machinery lives here so any test project can
/// reuse it by construction rather than by copy-paste: a concrete subclass only
/// names its <see cref="PackageAssembly"/>, hands back a serializer
/// <see cref="Services"/> provider (built with <c>AddSerializer()</c>), and
/// supplies the open <see cref="DeepCopierType"/> (<c>typeof(DeepCopier&lt;&gt;)</c>).
/// Because the type list is rebuilt from reflection on every run, a newly added
/// serializable exception is audited automatically - a future violation fails
/// <c>build-and-test</c> rather than surfacing in production.
/// </para>
/// <para>
/// This library stays product- and serializer-agnostic: it references no Orleans
/// type at compile time and drives the copier purely through
/// <see cref="System.Reflection"/> and <see cref="IServiceProvider"/>. The base is
/// <see langword="abstract"/> so it is never discovered on its own; the inherited
/// <c>[Test]</c> runs through the concrete subclass in the consuming assembly.
/// </para>
/// </summary>
public abstract class SerializableExceptionDeepCopyContractTestsBase
{
    /// <summary>
    /// The package assembly whose <c>[GenerateSerializer]</c> exception types are
    /// audited. Only types <em>declared</em> in this assembly are considered, so
    /// each package audits exactly its own exceptions.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// A service provider configured with Orleans serialization
    /// (<c>new ServiceCollection().AddSerializer().BuildServiceProvider()</c>) that
    /// can resolve the closed <see cref="DeepCopierType"/> for each audited type.
    /// </summary>
    protected abstract IServiceProvider Services { get; }

    /// <summary>
    /// The open generic Orleans deep-copier service type, i.e.
    /// <c>typeof(Orleans.Serialization.Cloning.DeepCopier&lt;&gt;)</c>. Supplied by
    /// the consumer so this library needs no compile-time Orleans reference.
    /// </summary>
    protected abstract Type DeepCopierType { get; }

    /// <summary>
    /// For every <c>[GenerateSerializer]</c> exception declared in
    /// <see cref="PackageAssembly"/>, resolves its Orleans deep-copier and copies a
    /// probe instance, asserting the copy succeeds and preserves the concrete type.
    /// A type deriving from a BCL exception subclass without a no-op copier fails
    /// here with the same <c>KeyNotFoundException</c> it would raise in production.
    /// </summary>
    [Test]
    public void Every_serializable_exception_deep_copies_on_a_same_silo_boundary()
    {
        var audited = 0;
        var offenders = new List<string>();

        foreach (var type in SerializableExceptionTypes())
        {
            audited++;

            if (!TryCreateProbe(type, out var instance, out var constructionError))
            {
                offenders.Add($"{type.FullName}: could not construct a probe instance ({constructionError})");
                continue;
            }

            var closedCopierType = DeepCopierType.MakeGenericType(type);
            var copier = Services.GetService(closedCopierType);
            if (copier is null)
            {
                offenders.Add($"{type.FullName}: no {closedCopierType.Name} registered");
                continue;
            }

            var copyMethod = closedCopierType.GetMethod("Copy", new[] { type });
            if (copyMethod is null)
            {
                offenders.Add($"{type.FullName}: {closedCopierType.Name} has no single-argument Copy method");
                continue;
            }

            try
            {
                var copy = copyMethod.Invoke(copier, new[] { instance });
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
                var inner = Unwrap(ex);
                offenders.Add($"{type.FullName}: {inner.GetType().Name}: {inner.Message}");
            }
        }

        Assert.That(audited, Is.GreaterThan(0),
            $"No [GenerateSerializer] exception types were discovered in "
            + $"{PackageAssembly.GetName().Name}; the guard would be inert. Verify PackageAssembly.");

        Assert.That(offenders, Is.Empty,
            "Every [GenerateSerializer] exception must deep-copy across a same-silo (co-located) "
            + "grain boundary. An exception deriving from a BCL exception other than System.Exception "
            + "needs a no-op [RegisterCopier] IDeepCopier<T> next to it (Orleans registers a copier "
            + "for System.Exception but not for its subclasses), or must derive from System.Exception "
            + "directly. Offenders:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The concrete, non-abstract <c>[GenerateSerializer]</c> exception types
    /// declared in <see cref="PackageAssembly"/>, ordered for a stable report.
    /// </summary>
    protected IEnumerable<Type> SerializableExceptionTypes() =>
        PackageAssembly.GetTypes()
            .Where(t => typeof(Exception).IsAssignableFrom(t)
                && !t.IsAbstract
                && !t.ContainsGenericParameters
                && t.GetCustomAttributes(inherit: false)
                    .Any(a => a.GetType().Name == "GenerateSerializerAttribute"))
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

    private static bool TryCreateProbe(Type type, out object instance, out string error)
    {
        instance = null!;
        error = string.Empty;
        try
        {
            var parameterless = type.GetConstructor(Type.EmptyTypes);
            if (parameterless is not null)
            {
                instance = parameterless.Invoke(null);
                return true;
            }

            var messageCtor = type.GetConstructor(new[] { typeof(string) });
            if (messageCtor is not null)
            {
                instance = messageCtor.Invoke(new object[] { "same-silo copy probe" });
                return true;
            }

            var created = Activator.CreateInstance(type, nonPublic: true);
            if (created is not null)
            {
                instance = created;
                return true;
            }

            error = "no usable constructor";
            return false;
        }
        catch (Exception ex)
        {
            error = Unwrap(ex).GetType().Name;
            return false;
        }
    }

    private static Exception Unwrap(Exception ex) =>
        ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
}
