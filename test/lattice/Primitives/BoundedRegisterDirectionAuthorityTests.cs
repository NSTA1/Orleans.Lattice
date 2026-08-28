using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regressions pinning the <b>authority</b> for a bounded register's fold
/// direction. Direction lives in two places - the registered
/// <see cref="LatticeMergeMode"/> for the key, and the
/// <see cref="BoundedRegister.IsMin"/> bit carried on the state - and only the
/// mode is authoritative. The state bit is a wire-carried cache of it.
/// <para>
/// The two can disagree whenever state reaches the store without passing
/// through a directional accessor: a raw byte write of hand-authored JSON, a
/// payload from a foreign or older writer, or a key whose mode was
/// re-registered. Left unstamped, such a payload folds under the wrong
/// direction indefinitely and silently, because
/// <see cref="BoundedRegister.MergeFrom(BoundedRegister)"/> resolves under the
/// <em>receiver's</em> direction and never inspects the other side's - so
/// nothing in the fold detects the disagreement. Every decode seam therefore
/// re-stamps the bit from the mode.
/// </para>
/// </summary>
[TestFixture]
public class BoundedRegisterDirectionAuthorityTests
{
    private static byte[] Bytes(params byte[] values) => values;

    private static byte[] EncodeWithDirection(bool isMin, byte[] value, byte[] orderKey)
    {
        var register = new BoundedRegister { IsMin = isMin, Value = value, OrderKey = orderKey, HasValue = true };
        return CrdtShape.ForMinRegister().SerializeState(register);
    }

    [Test]
    public void Min_shape_restamps_a_payload_that_claims_max()
    {
        // The exact hostile shape: a payload persisted with IsMin = false landing
        // on a key registered as a Min register.
        var bytes = EncodeWithDirection(isMin: false, Bytes(5), Bytes(5));

        var decoded = (BoundedRegister)CrdtShape.ForMinRegister().DeserializeState(bytes);

        Assert.That(decoded.IsMin, Is.True,
            "the registered merge mode is the authority; the stored bit must not win");
    }

    [Test]
    public void Max_shape_restamps_a_payload_that_claims_min()
    {
        var bytes = EncodeWithDirection(isMin: true, Bytes(5), Bytes(5));

        var decoded = (BoundedRegister)CrdtShape.ForMaxRegister().DeserializeState(bytes);

        Assert.That(decoded.IsMin, Is.False);
    }

    [Test]
    public void A_restamped_min_register_folds_downwards()
    {
        // The behaviour the stamp exists to protect: without it the decoded
        // register keeps IsMin = false and folds as a Max, so a lower candidate
        // is rejected and the register answers with the wrong extreme forever.
        var bytes = EncodeWithDirection(isMin: false, Bytes(5), Bytes(5));
        var decoded = (BoundedRegister)CrdtShape.ForMinRegister().DeserializeState(bytes);

        var lower = BoundedRegister.CreateEmpty(isMin: true);
        lower.Set(Bytes(1), Bytes(1));
        decoded.MergeFrom(lower);

        Assert.That(decoded.Value, Is.EqualTo(Bytes(1)).AsCollection,
            "a Min register must adopt the lesser candidate after decoding");
    }

    [Test]
    public void A_restamped_max_register_folds_upwards()
    {
        var bytes = EncodeWithDirection(isMin: true, Bytes(5), Bytes(5));
        var decoded = (BoundedRegister)CrdtShape.ForMaxRegister().DeserializeState(bytes);

        var higher = BoundedRegister.CreateEmpty(isMin: false);
        higher.Set(Bytes(9), Bytes(9));
        decoded.MergeFrom(higher);

        Assert.That(decoded.Value, Is.EqualTo(Bytes(9)).AsCollection);
    }

    [Test]
    public void An_agreeing_payload_decodes_unchanged()
    {
        var bytes = EncodeWithDirection(isMin: true, Bytes(5), Bytes(5));

        var decoded = (BoundedRegister)CrdtShape.ForMinRegister().DeserializeState(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.IsMin, Is.True);
            Assert.That(decoded.Value, Is.EqualTo(Bytes(5)).AsCollection);
            Assert.That(decoded.HasValue, Is.True);
        });
    }

    [Test]
    public void WithDirection_returns_the_same_instance_so_the_stamp_allocates_nothing()
    {
        var register = BoundedRegister.CreateEmpty(isMin: false);

        var stamped = register.WithDirection(true);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(register, stamped), Is.True,
                "the stamp must be an in-place write, not a second allocation");
            Assert.That(stamped.IsMin, Is.True);
        });
    }

    [Test]
    public void An_empty_register_from_the_shape_already_carries_the_direction()
    {
        var empty = (BoundedRegister)CrdtShape.ForMinRegister().CreateEmpty();

        Assert.That(empty.IsMin, Is.True);
    }
}
