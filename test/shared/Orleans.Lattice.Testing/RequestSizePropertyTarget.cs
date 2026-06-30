using System.Reflection;

namespace Orleans.Lattice.Testing;

/// <summary>
/// A single reflection-discovered audit target for a request-object surface: one
/// public method that takes a request DTO, paired with a size- or limit-like
/// <see cref="int"/> property on that DTO. One target is produced per such
/// property per method, so a request type with two size properties reached
/// through one method yields two targets, and the same request type reached
/// through two methods yields a target per method (each method is exercised).
/// Drives the request-object size contract guard exercised by
/// <see cref="RequestSizeContractTestsBase{TSelf}"/>.
/// </summary>
/// <param name="ApiType">The interface or class the method was discovered on.</param>
/// <param name="Method">The public method under audit.</param>
/// <param name="RequestParameter">The request-DTO parameter of <paramref name="Method"/>.</param>
/// <param name="RequestType">The request DTO type carrying the size property.</param>
/// <param name="SizeProperty">The size/limit <see cref="int"/> property being exercised.</param>
public sealed record RequestSizePropertyTarget(
    Type ApiType,
    MethodInfo Method,
    ParameterInfo RequestParameter,
    Type RequestType,
    PropertyInfo SizeProperty)
{
    /// <summary>
    /// A stable, human-readable identifier of the form
    /// <c>ApiType.Method(RequestType.Property)</c> used for test-case names and
    /// assertion messages.
    /// </summary>
    public string DisplayName => $"{ApiType.Name}.{Method.Name}({RequestType.Name}.{SizeProperty.Name})";

    /// <inheritdoc />
    public override string ToString() => DisplayName;
}
