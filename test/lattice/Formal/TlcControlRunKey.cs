using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The identity of a TLC clean-source control run: a hash of everything TLC
/// reads for it, so two pairing cases share a control exactly when their
/// complete inputs are identical (see <see cref="TlcControlRunCache{TResult}"/>).
/// <para>
/// The inputs are the module's specification path, the unmutated base source,
/// the full generated control cfg text, the TLC options, and every sibling
/// <c>.tla</c> (name and text, in ordinal name order) copied into the scratch
/// directory beside it. A difference in any of them is a different run.
/// </para>
/// <para>
/// The mutation's name is deliberately NOT an input. A control checks the
/// unmutated base, so it is the same experiment for every mutation that agrees
/// on module, target, bounds and options. The control cfgs
/// (<see cref="SpecMutation.BuildConfig(string)"/> and
/// <see cref="SpecMutation.BuildMatchingControlConfig"/>) therefore do not name
/// the mutation; before #5152 they did, every key was unique, and a full run
/// launched 1,180 controls where 597 were distinct.
/// </para>
/// </summary>
internal static class TlcControlRunKey
{
    /// <summary>The control key for a module's base specification under a generated control cfg.</summary>
    /// <param name="module">The module whose unmutated base the control checks.</param>
    /// <param name="baseSpecification">The module's base specification text.</param>
    /// <param name="config">The generated control cfg text.</param>
    /// <param name="options">The extra TLC options the control runs with.</param>
    public static string For(SpecModule module, string baseSpecification, string config, IReadOnlyList<string> options)
    {
        ArgumentNullException.ThrowIfNull(module);

        return Compute(module.SpecificationPath, baseSpecification, config, options, module.ReadSiblingSpecifications());
    }

    /// <summary>The control key over explicit inputs, so it can be computed without a module on disk.</summary>
    /// <param name="specificationPath">The path of the module's specification.</param>
    /// <param name="baseSpecification">The module's base specification text.</param>
    /// <param name="config">The generated control cfg text.</param>
    /// <param name="options">The extra TLC options the control runs with.</param>
    /// <param name="siblings">Every <c>.tla</c> beside the module, keyed by file name.</param>
    public static string Compute(
        string specificationPath,
        string baseSpecification,
        string config,
        IReadOnlyList<string> options,
        IReadOnlyDictionary<string, string> siblings)
    {
        ArgumentException.ThrowIfNullOrEmpty(specificationPath);
        ArgumentNullException.ThrowIfNull(baseSpecification);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(siblings);

        var key = new StringBuilder()
            .Append(specificationPath).Append('\0')
            .Append(baseSpecification).Append('\0')
            .Append(config).Append('\0')
            .AppendJoin(' ', options).Append('\0');
        foreach (var (name, text) in siblings.OrderBy(s => s.Key, StringComparer.Ordinal))
        {
            key.Append(name).Append('\0').Append(text).Append('\0');
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key.ToString())));
    }
}
