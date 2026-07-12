namespace Orleans.Lattice.Schema;

/// <summary>
/// The pure, per-value re-stamping step of an eager schema-version migration: strip
/// a stored value's schema-version envelope, upcast its body from <b>its own</b>
/// stamped version to the tree's target version through the registered upcaster
/// chain, and re-envelope it at the target. It is the versioning analogue of
/// <see cref="LatticeValueTransformEvaluation"/>, and the per-value function the
/// background shadow build dispatches through in
/// <see cref="SchemaRemediationMode.SchemaVersionMigration"/> mode.
/// </summary>
/// <remarks>
/// <para>
/// <b>Idempotence.</b> A value already stamped at the target version is returned
/// unchanged, so a repeated migration (or a failover-resumed build) re-evaluates
/// identically. Re-stamping only rewrites the envelope version and upcasts the
/// body; the logical value a client observes after cutover is byte-identical to
/// what read-time lazy upcasting would have returned before it, because upcasting
/// is a total, deterministic function of <c>(schemaId, fromVersion, toVersion)</c>.
/// </para>
/// <para>
/// <b>Abort contract.</b> A value that cannot be upcast to the target - no
/// registered hop, or a version <i>newer</i> than the target - surfaces the schema
/// registry's <see cref="NotSupportedException"/>, which the shadow build catches
/// and turns into an abort naming the offending key and value preview, leaving the
/// original tree untouched. A <c>null</c> value surfaces
/// <see cref="InvalidOperationException"/>, mirroring the transform evaluator's
/// malformed-value abort.
/// </para>
/// <para>
/// <b>Legacy un-stamped values.</b> A value with no envelope (a legacy value
/// written before the tree opted in) is stamped at the target version with its body
/// untouched: the lazy read path returns such a value verbatim, so stamping it at
/// the target preserves read determinism (a later read strips the target envelope
/// back to the same body).
/// </para>
/// </remarks>
internal static class LatticeSchemaVersionMigration
{
    /// <summary>
    /// Re-stamps <paramref name="value"/> to <paramref name="targetVersion"/>.
    /// </summary>
    /// <param name="value">The stored value bytes (enveloped or legacy un-stamped).</param>
    /// <param name="schemaId">The schema-family id to stamp legacy un-enveloped values with.</param>
    /// <param name="targetVersion">The target schema version to re-stamp to.</param>
    /// <param name="registry">The schema registry providing the upcaster chain.</param>
    /// <returns>The re-stamped value bytes, or <paramref name="value"/> unchanged when it is already at the target.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="registry"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="value"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The value's stamped version cannot be upcast to <paramref name="targetVersion"/> (no registered hop, or it is newer than the target).</exception>
    internal static byte[] Migrate(byte[] value, uint schemaId, uint targetVersion, ILatticeSchemaRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        if (value is null)
        {
            throw new InvalidOperationException("Cannot migrate a null value.");
        }

        // A legacy un-stamped value (or an empty value) carries no version. The lazy
        // read path returns it verbatim, so stamping it at the target - without
        // upcasting its body - keeps a post-cutover read byte-identical.
        if (!LatticeSchemaEnvelope.IsEnveloped(value))
        {
            return LatticeSchemaEnvelope.Encode(schemaId, targetVersion, value);
        }

        _ = LatticeSchemaEnvelope.TryReadHeader(value, out var stampedSchemaId, out var storedVersion);

        // Already at the target: idempotent pass-through, no re-allocation.
        if (storedVersion == targetVersion)
        {
            return value;
        }

        // Upcast from the value's own stamped version through the registered chain,
        // then re-envelope at the target. A missing hop or a newer-than-target value
        // throws NotSupportedException, which the shadow build turns into an abort.
        var body = LatticeSchemaEnvelope.StripToBody(value);
        var upcast = registry.Upcast(stampedSchemaId, storedVersion, targetVersion, body);
        return LatticeSchemaEnvelope.Encode(stampedSchemaId, targetVersion, upcast);
    }
}
