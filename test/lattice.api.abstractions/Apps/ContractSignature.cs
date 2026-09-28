using System.Reflection;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

/// <summary>Renders an interface method as a stable, comparable signature string.</summary>
internal static class ContractSignature
{
    /// <summary>Renders the return type, name, and each parameter's type, name and optionality.</summary>
    /// <param name="method">The method to render.</param>
    /// <returns>A signature such as <c>Task&lt;AppCatalog&gt; ListAsync(CancellationToken cancellationToken = default)</c>.</returns>
    public static string Render(MethodInfo method) =>
        $"{RenderType(method.ReturnType)} {method.Name}("
        + string.Join(", ", method.GetParameters().Select(p =>
            $"{RenderType(p.ParameterType)} {p.Name}{(p.HasDefaultValue ? " = default" : string.Empty)}"))
        + ")";

    private static string RenderType(Type type) => type.IsGenericType
        ? $"{type.Name[..type.Name.IndexOf('`')]}<{string.Join(", ", type.GetGenericArguments().Select(RenderType))}>"
        : type.Name;
}
