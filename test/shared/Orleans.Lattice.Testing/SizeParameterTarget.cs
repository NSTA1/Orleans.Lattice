using System.Reflection;

namespace Orleans.Lattice.Testing;

/// <summary>
/// A single reflection-discovered audit target: one public method that takes a
/// size- or limit-like <see cref="int"/> parameter, paired with that parameter.
/// One target is produced per such parameter, so a method with two size
/// parameters yields two targets. Drives the table-style public-API size
/// contract guard exercised by <see cref="PublicApiSizeContractTestsBase"/>.
/// </summary>
/// <param name="ApiType">The interface or class the method was discovered on.</param>
/// <param name="Method">The public method under audit.</param>
/// <param name="Parameter">The size/limit <see cref="int"/> parameter of <paramref name="Method"/> being exercised.</param>
public sealed record SizeParameterTarget(Type ApiType, MethodInfo Method, ParameterInfo Parameter)
{
    /// <summary>
    /// A stable, human-readable identifier of the form
    /// <c>ApiType.Method(parameter)</c> used for test-case names and assertion
    /// messages.
    /// </summary>
    public string DisplayName => $"{ApiType.Name}.{Method.Name}({Parameter.Name})";

    /// <inheritdoc />
    public override string ToString() => DisplayName;
}
