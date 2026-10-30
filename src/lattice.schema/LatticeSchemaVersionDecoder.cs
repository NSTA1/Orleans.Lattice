namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-versioning <see cref="ILatticeValueDecoder"/>. At the client-facing
/// read boundary it strips the per-value schema-version envelope and, when the
/// stored version is older than the tree's target, upcasts the body through the
/// registered upcaster chain before returning it - so a caller always receives the
/// current-shape plain bytes regardless of which version each key was stored at.
/// </summary>
/// <remarks>
/// <para>
/// <b>Self-describing dispatch.</b> The decoder is active for every tree once
/// versioning is registered, but it dispatches on each value's own envelope rather
/// than on per-tree state, so it is safe on the resolver-less replay / restore
/// path: an un-stamped (legacy or unversioned) value is recognized by the absence
/// of the envelope magic and returned verbatim after a single leading-byte check,
/// while a stamped value carries its own <c>(schemaId, version)</c> and is upcast
/// from that version. Making the decoder self-dispatching (rather than gating
/// <see cref="IsActive"/> on a per-tree opt-in set) is deliberate: a lagging silo
/// that has not yet learned a tree opted in must still strip an enveloped value it
/// reads, never return raw enveloped bytes to a client.
/// </para>
/// <para>
/// <b>Cost.</b> With versioning unregistered the core registers the no-op decoder
/// and the read path is byte-for-byte identical (zero cost). With versioning
/// registered, a read of an un-stamped value pays one leading-byte comparison and
/// returns the stored array unchanged; only a stamped value pays the config lookup
/// (cached) and, when stale, the upcast.
/// </para>
/// <para>
/// <b>Unknown-newer throw.</b> A value stamped at a version <i>newer</i> than the
/// tree's target - or one whose version cannot be upcast to the target - surfaces
/// <see cref="NotSupportedException"/>, mirroring the unknown-compressor case.
/// </para>
/// </remarks>
internal sealed class LatticeSchemaVersionDecoder(
    ILatticeSchemaVersionProvider provider,
    ILatticeSchemaRegistry registry,
    SchemaRuntimeRegistry? runtimeRegistry = null) : ILatticeValueDecoder
{
    /// <inheritdoc />
    public bool IsActive(string treeId) => true;

    /// <inheritdoc />
    public ValueTask<byte[]> DecodeAsync(string treeId, byte[] storedValue, CancellationToken ct)
    {
        // Fast path: an un-stamped value (unversioned tree, or a legacy value written
        // before the tree opted in) is returned verbatim after one leading-byte check
        // with no config lookup and no allocation.
        if (!LatticeSchemaEnvelope.IsEnveloped(storedValue))
        {
            return new ValueTask<byte[]>(storedValue);
        }

        return DecodeEnvelopedAsync(treeId, storedValue, ct);
    }

    private async ValueTask<byte[]> DecodeEnvelopedAsync(string treeId, byte[] storedValue, CancellationToken ct)
    {
        _ = LatticeSchemaEnvelope.TryReadHeader(storedValue, out var schemaId, out var storedVersion);
        var body = LatticeSchemaEnvelope.StripToBody(storedValue);

        var config = await provider.GetConfigAsync(treeId, ct).ConfigureAwait(false);

        // Without a matching config the target version is unknown; the value is still
        // self-describing, so return its stored-version body rather than guessing.
        if (config is not { } version || version.SchemaId != schemaId)
        {
            return body;
        }

        if (storedVersion == version.TargetVersion)
        {
            return body;
        }

        // Stale value: upcast the body to the target before returning it.
        return await UpcastToTargetAsync(treeId, schemaId, storedVersion, version.TargetVersion, body)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Decodes <paramref name="storedValue"/> exactly as <see cref="DecodeAsync"/>
    /// does, and also reports the schema-version stamp the decoded body is at, so a
    /// transform remediation can re-envelope its rewrite at that stamp instead of
    /// writing it bare (issue #5243).
    /// </summary>
    /// <remarks>
    /// The stamp is the version the transform runs on: the tree's target for a value
    /// of the tree's schema family (a legacy un-stamped value included, which a read
    /// returns verbatim and an eager migration stamps at the target unchanged); the
    /// value's own stamp for a value of another family, which a read returns at its
    /// stored version. An unversioned tree reports no stamp, so its remediation keeps
    /// writing plain values. A value already at the target is returned as a view past
    /// the envelope header with no copy.
    /// </remarks>
    /// <param name="treeId">The logical tree the value was read from.</param>
    /// <param name="storedValue">The stored (possibly enveloped) value bytes.</param>
    /// <param name="ct">Cancels the config lookup.</param>
    /// <returns>The decoded body and its stamp.</returns>
    /// <exception cref="NotSupportedException">The value is stamped newer than the tree's target, or cannot be upcast to it.</exception>
    internal async ValueTask<SchemaRestampInput> DecodeForRestampAsync(
        string treeId, byte[] storedValue, CancellationToken ct)
    {
        var config = await provider.GetConfigAsync(treeId, ct).ConfigureAwait(false);
        if (!LatticeSchemaEnvelope.TryReadHeader(storedValue, out var schemaId, out var storedVersion))
        {
            return config is { } target
                ? new SchemaRestampInput(storedValue, Stamped: true, target.SchemaId, target.TargetVersion)
                : new SchemaRestampInput(storedValue, Stamped: false, 0, 0);
        }

        var body = storedValue.AsMemory(LatticeSchemaEnvelope.HeaderLength);
        if (config is not { } version)
        {
            return new SchemaRestampInput(body, Stamped: false, 0, 0);
        }

        if (version.SchemaId != schemaId || storedVersion == version.TargetVersion)
        {
            return new SchemaRestampInput(body, Stamped: true, schemaId, storedVersion);
        }

        var upcast = await UpcastToTargetAsync(
            treeId, schemaId, storedVersion, version.TargetVersion, LatticeSchemaEnvelope.StripToBody(storedValue))
            .ConfigureAwait(false);
        return new SchemaRestampInput(upcast, Stamped: true, schemaId, version.TargetVersion);
    }

    private async ValueTask<byte[]> UpcastToTargetAsync(
        string treeId, uint schemaId, uint storedVersion, uint targetVersion, byte[] body)
    {
        if (storedVersion > targetVersion)
        {
            throw new NotSupportedException(
                $"Value for tree '{treeId}' is stamped at schema {schemaId} v{storedVersion}, newer than the tree's " +
                $"target v{targetVersion}. Upgrade the reader's schema registry / target version to read it.");
        }

        var effectiveRegistry = runtimeRegistry is null ? registry
            : await runtimeRegistry.ResolveAsync(treeId).ConfigureAwait(false);
        return effectiveRegistry.Upcast(schemaId, storedVersion, targetVersion, body);
    }
}
