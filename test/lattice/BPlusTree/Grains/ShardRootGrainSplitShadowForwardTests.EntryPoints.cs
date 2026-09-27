using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainSplitShadowForwardTests
{
    [Test]
    public void Split_shadow_write_has_one_single_key_forwarding_entry_point()
    {
        // Batch wrappers and resize forwarding have different signatures. Keep
        // single-key forwarding on the path whose saga/phase behaviour we test.
        var entryPoints = typeof(ShardRootGrain)
            .GetMethods(BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(method => method.Name.Contains("Shadow", StringComparison.Ordinal)
                && method.Name.Contains("Forward", StringComparison.Ordinal))
            .Where(method =>
            {
                var parameters = method.GetParameters();
                return parameters.Length >= 2
                    && parameters[0].ParameterType == typeof(string)
                    && (parameters[1].ParameterType == typeof(byte[])
                        || parameters[1].ParameterType == typeof(LwwValue<byte[]>));
            })
            .Select(method => method.Name)
            .ToArray();

        Assert.That(entryPoints, Is.EqualTo(new[] { "ForwardLocalWriteToShadowIfNeededAsync" }),
            "Keep one single-key split-shadow write path; do not restore a predecessor "
            + "that bypasses prepared writes, TTL, Reject or post-Complete forwarding.");
    }
}
