using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Renders an interface's members as exact, comparable signature strings: return
/// type and parameter types with their nullable annotations, parameter names,
/// <c>ref</c>/<c>out</c>/<c>in</c> modifiers, and the literal value of every
/// optional parameter's default. Two renderings are equal only when a caller
/// compiled against one binds unchanged against the other, so a pinned rendering
/// is a byte-identity proof of a released contract.
/// </summary>
internal static class InterfaceSignature
{
    private static readonly NullabilityInfoContext Nullability = new();

    /// <summary>Renders every member <paramref name="contract"/> declares, in metadata order.</summary>
    /// <param name="contract">The interface to render.</param>
    /// <returns>One rendering per declared method, property and event.</returns>
    public static IReadOnlyList<string> Render(Type contract)
    {
        var rendered = new List<string>();
        foreach (var member in contract.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            rendered.Add(member switch
            {
                MethodInfo method => RenderMethod(method),
                PropertyInfo property => $"property {RenderType(property.PropertyType, Nullability.Create(property))} {property.Name}",
                EventInfo evt => $"event {evt.EventHandlerType?.Name} {evt.Name}",
                _ => $"{member.MemberType} {member.Name}",
            });
        }

        return rendered;
    }

    private static string RenderMethod(MethodInfo method)
    {
        var builder = new StringBuilder();
        builder.Append(RenderType(method.ReturnType, Nullability.Create(method.ReturnParameter)))
            .Append(' ').Append(method.Name);
        if (method.IsGenericMethodDefinition)
        {
            builder.Append('<').Append(string.Join(", ", method.GetGenericArguments().Select(a => a.Name))).Append('>');
        }

        builder.Append('(');
        var parameters = method.GetParameters();
        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            if (i > 0)
            {
                builder.Append(", ");
            }

            var type = parameter.ParameterType;
            if (type.IsByRef)
            {
                builder.Append(parameter.IsOut ? "out " : parameter.IsIn ? "in " : "ref ");
                type = type.GetElementType()!;
            }

            builder.Append(RenderType(type, Nullability.Create(parameter))).Append(' ').Append(parameter.Name);
            if (parameter.HasDefaultValue)
            {
                builder.Append(" = ").Append(RenderDefault(parameter));
            }
        }

        return builder.Append(')').ToString();
    }

    private static string RenderDefault(ParameterInfo parameter) => parameter.RawDefaultValue switch
    {
        null when parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) is null => "default",
        null => "null",
        string s => $"\"{s}\"",
        bool b => b ? "true" : "false",
        var value when parameter.ParameterType.IsEnum || Nullable.GetUnderlyingType(parameter.ParameterType)?.IsEnum == true
            => $"{(Nullable.GetUnderlyingType(parameter.ParameterType) ?? parameter.ParameterType).Name}.{Enum.ToObject(Nullable.GetUnderlyingType(parameter.ParameterType) ?? parameter.ParameterType, value)}",
        var value => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "?",
    };

    private static string RenderType(Type type, NullabilityInfo nullability)
    {
        var annotation = !type.IsValueType && nullability.ReadState == NullabilityState.Nullable ? "?" : string.Empty;
        if (Nullable.GetUnderlyingType(type) is { } underlying)
        {
            return underlying.Name + "?";
        }

        if (type.IsArray)
        {
            return RenderType(type.GetElementType()!, nullability.ElementType!) + "[]" + annotation;
        }

        if (!type.IsGenericType)
        {
            return type.Name + annotation;
        }

        var arguments = type.GetGenericArguments();
        var rendered = new string[arguments.Length];
        for (var i = 0; i < arguments.Length; i++)
        {
            rendered[i] = RenderType(arguments[i], nullability.GenericTypeArguments[i]);
        }

        return $"{type.Name[..type.Name.IndexOf('`')]}<{string.Join(", ", rendered)}>{annotation}";
    }
}
