using System.Globalization;
using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Invocation and assertion helpers shared by the public-API size contract
/// guards (<see cref="PublicApiSizeContractTestsBase{TSelf}"/> for method
/// parameters and <see cref="RequestSizeContractTestsBase{TSelf}"/> for request
/// DTO properties). Both guards reflect a call, await whatever awaitable shape it
/// returns, and assert the call never faults with
/// <see cref="OutOfMemoryException"/> for a pathological size input; this type
/// holds that one shared mechanism so the two guards cannot drift.
/// </summary>
internal static class SizeContractAssertions
{
    /// <summary>
    /// Awaits the value returned by a reflected invocation, supporting
    /// <see cref="Task"/> / <see cref="Task{TResult}"/>,
    /// <see cref="ValueTask"/> / <see cref="ValueTask{TResult}"/>, and plain
    /// synchronous returns.
    /// </summary>
    public static async Task AwaitResultAsync(object? result)
    {
        switch (result)
        {
            case null:
                return;
            case Task task:
                await task.ConfigureAwait(false);
                return;
            case ValueTask valueTask:
                await valueTask.ConfigureAwait(false);
                return;
        }

        // ValueTask<T> is a distinct generic struct; bridge it to Task via its
        // AsTask() method so a single await path covers every awaitable shape.
        var resultType = result.GetType();
        if (resultType.IsGenericType
            && resultType.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            var asTask = resultType.GetMethod(nameof(ValueTask<int>.AsTask));
            if (asTask?.Invoke(result, null) is Task bridged)
            {
                await bridged.ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Fails the test when <paramref name="exception"/> is, or wraps, an
    /// <see cref="OutOfMemoryException"/>; otherwise the exception is an
    /// acceptable rejection of the pathological input and the test passes.
    /// </summary>
    /// <param name="exception">The exception thrown by the reflected call.</param>
    /// <param name="displayName">The audit target's display name, for the message.</param>
    /// <param name="boundaryValue">The pathological boundary value under test.</param>
    public static void AssertNoOutOfMemory(Exception exception, string displayName, int boundaryValue)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is OutOfMemoryException || current is AggregateException aggregate
                && aggregate.Flatten().InnerExceptions.Any(e => e is OutOfMemoryException))
            {
                Assert.Fail(
                    $"{displayName} threw OutOfMemoryException for "
                    + $"{Describe(boundaryValue)}; the size must be clamped before "
                    + "allocating so a pathological caller value cannot fault the host.");
            }
        }
    }

    /// <summary>The declared optional default of a parameter, otherwise the runtime default for its type.</summary>
    public static object? DefaultFor(ParameterInfo parameter)
    {
        if (parameter.HasDefaultValue)
        {
            return parameter.DefaultValue;
        }

        var type = parameter.ParameterType;
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>A readable name for a boundary value, used in test-case names and messages.</summary>
    public static string Describe(int value) => value switch
    {
        int.MaxValue => "int.MaxValue",
        int.MinValue => "int.MinValue",
        _ => value.ToString(CultureInfo.InvariantCulture),
    };
}
