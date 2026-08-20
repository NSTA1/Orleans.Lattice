# Wire format

Schema [versioning](schema-versioning.md) stamps each stored value with a small,
fixed-layout envelope header so a reader can tell which schema version a value was
written at *before* it interprets the body. This page documents that header. The
layout is **frozen**: it is on-disk and on-the-wire contract, and it never changes
for a given `FormatVersion`.

## Envelope header

An envelope is a 10-byte big-endian header followed by the plain value body:

| Offset | Size | Field | Value / meaning |
|---|---|---|---|
| 0 | 1 | `Magic` | `0xFE` - a reserved discriminator that is **not** a valid UTF-8 lead byte. |
| 1 | 1 | `FormatVersion` | `0x01` - the envelope-format version (distinct from the per-value schema version). |
| 2..5 | 4 | schema id | `uint`, big-endian - which logical schema family the value belongs to. |
| 6..9 | 4 | schema version | `uint`, big-endian - the version the body's shape conforms to. |
| 10.. | n | body | The plain value bytes (UTF-8 / JSON). |

The relevant constants are `LatticeSchemaEnvelope.Magic`,
`LatticeSchemaEnvelope.FormatVersion`, and `LatticeSchemaEnvelope.HeaderLength`.

## Default omission

The envelope is **default-omitted**. A value written to an opted-out tree, or a
value whose tree is versioned but is being stored unstamped (target version 0),
carries **zero** extra bytes - its byte shape is byte-for-byte identical to a plain
lattice value. This is what keeps versioning zero-overhead when unused and keeps a
migration incremental: stamped and un-stamped values coexist in the same tree.

## Why `0xFE`

`0xFE` is never a valid UTF-8 lead byte, so a stored UTF-8 or JSON body never
begins with it. That makes an un-stamped legacy value unambiguously distinguishable
from a stamped one on read: the decoder treats a value that does not start with the
magic and a recognised format version as an unstamped body and returns it verbatim.

**Discriminator caveat.** The single-byte magic cleanly disambiguates UTF-8 / JSON
bodies. An arbitrary binary blob whose first bytes legitimately begin with the
magic-and-format-version pattern cannot be perfectly distinguished from a real
envelope. Schema versioning therefore targets text / UTF-8 payloads; do not enable
it on trees storing arbitrary opaque binary values.

## Forward compatibility

A value stamped with a **newer** schema version than a reader's target version - or
one whose stored version cannot be upcast to the target - surfaces
`NotSupportedException` on read, the same behaviour as an unknown compressor. This
is deliberate: rather than silently mis-decode, the reader fails loudly so an
operator upgrades the reader's registry or target version.

An **unrecognised format version** is handled differently: it does **not** throw.
The envelope check requires *both* the magic byte and the recognised
`FormatVersion` (`0x01`), so a value whose second byte is any other format version
fails the check, is treated as an un-stamped (plain) body, and is passed through
verbatim. The reserved `FormatVersion` byte therefore lets a future header shape
coexist with `0x01`: a reader that does not recognise the newer format falls
through and returns the payload unchanged rather than failing.

## See also

- [Schema versioning](schema-versioning.md) - the capability that writes the envelope.
