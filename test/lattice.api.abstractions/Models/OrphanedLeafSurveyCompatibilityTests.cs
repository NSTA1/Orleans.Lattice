using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>Compiles legacy implementers without the survey member and exercises the loud default.</summary>
[TestFixture]
public sealed class OrphanedLeafSurveyCompatibilityTests
{
    [TestCase(typeof(ILattice))]
    [TestCase(typeof(ILatticeTreeAdmin))]
    public void Legacy_implementer_without_survey_compiles_and_reports_unsupported(Type contract)
    {
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Concat(Directory.EnumerateFiles(AppContext.BaseDirectory, "Orleans*.dll"))
            .Distinct(StringComparer.OrdinalIgnoreCase);
        var compilation = CSharpCompilation.Create(
            $"LegacySurvey_{Guid.NewGuid():N}",
            references: paths.Select(path => MetadataReference.CreateFromFile(path)),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var symbol = compilation.GetTypeByMetadataName(contract.FullName!);
        Assert.That(symbol, Is.Not.Null);

        // Generate only the old required members, regardless of whether survey
        // accidentally becomes abstract. Such a regression must fail compilation.
        var methods = symbol!.AllInterfaces.Append(symbol).SelectMany(type => type.GetMembers())
            .OfType<IMethodSymbol>().Where(method => method.IsAbstract && method.Name != "SurveyOrphanedLeavesAsync");
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
        var source = $"public sealed class Legacy : {contract.FullName} {{ {string.Join("\n", members)} }}";
        compilation = compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText(source));
        using var image = new MemoryStream();
        var emitted = compilation.Emit(image);
        Assert.That(emitted.Success, Is.True, string.Join("\n", emitted.Diagnostics));
        var legacyType = Assembly.Load(image.ToArray()).GetType("Legacy")!;
        Assert.That(legacyType.GetMethod("SurveyOrphanedLeavesAsync"), Is.Null);
        var legacy = Activator.CreateInstance(legacyType)!;
        var error = contract == typeof(ILattice)
            ? Assert.Throws<NotSupportedException>(() => { _ = ((ILattice)legacy).SurveyOrphanedLeavesAsync(); })
            : Assert.Throws<NotSupportedException>(() => { _ = ((ILatticeTreeAdmin)legacy).SurveyOrphanedLeavesAsync("tree"); });
        Assert.That(error!.Message, Does.Contain("SurveyOrphanedLeavesAsync"));
    }
}
