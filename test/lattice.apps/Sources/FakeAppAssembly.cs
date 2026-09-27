using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An assembly stand-in that serves only embedded resources and throws on any access to its type
/// system, so a test can prove that resolving a manifest touches no app code.
/// </summary>
internal sealed class FakeAppAssembly(Func<string, Stream?> open) : Assembly
{
    private int resourceReads;

    public FakeAppAssembly(string resourceName, string json)
        : this(name => name == resourceName ? new MemoryStream(Encoding.UTF8.GetBytes(json)) : null)
    {
    }

    public int ResourceReads => Volatile.Read(ref resourceReads);

    public override Stream? GetManifestResourceStream(string name)
    {
        Interlocked.Increment(ref resourceReads);
        return open(name);
    }

    public override Type[] GetTypes() => throw CodeTouched();

    public override Type[] GetExportedTypes() => throw CodeTouched();

    public override IEnumerable<TypeInfo> DefinedTypes => throw CodeTouched();

    public override MethodInfo? EntryPoint => throw CodeTouched();

    public override Type? GetType(string name, bool throwOnError, bool ignoreCase) => throw CodeTouched();

    public override Module[] GetModules(bool getResourceModules) => throw CodeTouched();

    private static InvalidOperationException CodeTouched() =>
        new("Resolving a manifest must not touch app code.");
}
