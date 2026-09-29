using System.Reflection;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The service key the Explorer registers and resolves its facades under.
/// </summary>
/// <remarks>
/// The key is internal to the Explorer UI, and this project is deliberately not a
/// friend of it, so it is read from the Explorer's own declaration rather than copied:
/// a copied literal would keep compiling after a rename while every facade this suite
/// registers silently stopped being the one the Explorer resolves.
/// </remarks>
internal static class ExplorerFacadeKey
{
    private const string DeclaringType = "Orleans.Lattice.Explorer.UI.Transport.ShellFacades";

    /// <summary>The key.</summary>
    public static string Value { get; } = Read();

    private static string Read()
    {
        var type = typeof(ShellLayout).Assembly.GetType(DeclaringType, throwOnError: false)
            ?? throw new InvalidOperationException($"The Explorer UI no longer declares {DeclaringType}; the suite cannot register its facades under the Explorer's key.");
        var field = type.GetField("Key", BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"{DeclaringType} no longer declares a public Key constant.");
        return (string)field.GetRawConstantValue()!;
    }
}
