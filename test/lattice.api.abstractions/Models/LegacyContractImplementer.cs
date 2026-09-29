using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Compiles, at run time, a type that implements only the <em>abstract</em>
/// members of a contract interface - the shape a legacy implementer written
/// against an earlier version of that contract takes.
/// </summary>
/// <remarks>
/// <para>
/// A default interface member is deliberately never generated, so the compiled
/// type inherits the contract's own default implementation. That is what makes
/// this the only way to execute such a default body: every hand-written or
/// substituted implementer in the repository overrides it, and a mocking proxy
/// intercepts the member rather than dispatching to the interface, so neither
/// reaches the default at all.
/// </para>
/// <para>
/// Generating from the abstract surface rather than from a fixed member list is
/// load-bearing. Should a member that is meant to stay default ever become
/// abstract, this generator emits an implementation for it and the fixture that
/// asserts the default behaviour stops proving anything; the callers below guard
/// that by asserting the generated type declares no such member.
/// </para>
/// </remarks>
internal static class LegacyContractImplementer
{
    /// <summary>The name the generated type is emitted under.</summary>
    internal const string TypeName = "Legacy";

    /// <summary>
    /// Emits and loads a type implementing every abstract member of
    /// <paramref name="contract"/>, each throwing
    /// <see cref="NotImplementedException"/>.
    /// </summary>
    /// <param name="contract">The contract interface to implement.</param>
    /// <param name="excludedMembers">
    /// Member names to leave unimplemented even when they are abstract. A caller
    /// passes a name here to assert that the member is <em>not</em> abstract:
    /// were it to become so, the generated source would no longer satisfy the
    /// interface and the emit below fails loudly rather than silently changing
    /// what the fixture proves.
    /// </param>
    /// <returns>The loaded generated type.</returns>
    internal static Type Compile(Type contract, params string[] excludedMembers)
    {
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Concat(Directory.EnumerateFiles(AppContext.BaseDirectory, "Orleans*.dll"))
            .Distinct(StringComparer.OrdinalIgnoreCase);
        var compilation = CSharpCompilation.Create(
            $"LegacyContract_{Guid.NewGuid():N}",
            references: paths.Select(path => MetadataReference.CreateFromFile(path)),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var symbol = compilation.GetTypeByMetadataName(contract.FullName!);
        Assert.That(symbol, Is.Not.Null, $"The contract '{contract.FullName}' did not resolve against the test references.");

        var methods = symbol!.AllInterfaces.Append(symbol).SelectMany(type => type.GetMembers())
            .OfType<IMethodSymbol>()
            .Where(method => method.IsAbstract && !excludedMembers.Contains(method.Name, StringComparer.Ordinal));
        var members = methods.Select(method =>
        {
            Assert.That(method.IsGenericMethod, Is.False);
            Assert.That(method.MethodKind, Is.EqualTo(MethodKind.Ordinary));
            var parameters = string.Join(", ", method.Parameters.Select(parameter =>
            {
                Assert.That(parameter.RefKind, Is.EqualTo(RefKind.None));
                return $"{parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)} @{parameter.Name}";
            }));
            return $"{method.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)} "
                + $"{method.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}.{method.Name}({parameters}) "
                + "=> throw new global::System.NotImplementedException();";
        });
        var source = $"public sealed class {TypeName} : {contract.FullName} {{ {string.Join("\n", members)} }}";
        compilation = compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText(source));
        using var image = new MemoryStream();
        var emitted = compilation.Emit(image);
        Assert.That(emitted.Success, Is.True, string.Join("\n", emitted.Diagnostics));
        return Assembly.Load(image.ToArray()).GetType(TypeName)!;
    }

    /// <summary>
    /// Emits, loads, and instantiates a legacy implementer of
    /// <paramref name="contract"/>.
    /// </summary>
    internal static object CreateInstance(Type contract, params string[] excludedMembers) =>
        Activator.CreateInstance(Compile(contract, excludedMembers))!;
}
