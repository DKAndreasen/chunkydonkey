import clevercsv
import io
import polars as pl


def tabular_to_markdown(file: bytes):

    try: # Parquet?
        df = pl.read_parquet(io.BytesIO(file))
        meta = {'content_type': 'application/vnd.apache.parquet'}
    except Exception:
        try: # Regular JSON?
            df = pl.read_json(io.BytesIO(file))
            meta = {'content_type': 'application/json'}
        except Exception:
            try: # NDJSON?
                df = pl.read_ndjson(io.BytesIO(file))
                meta = {'content_type': 'application/json'}
            except Exception:
                # CSV? (no catching exceptions)
                # Decode and detect dialect (delimiter, quotechar, escapechar)
                text = file.decode('utf-8', errors='replace')
                sniffer = clevercsv.Sniffer()
                dialect = sniffer.sniff(text, verbose=False)
                if dialect is None or dialect.delimiter is None:
                    raise ValueError
                # Detect if file has header
                has_header = sniffer.has_header(text)
                # Map CleverCSV dialect to Polars parameters ('' -> None)
                quote_char = dialect.quotechar if dialect.quotechar != '' else None
                # Read CSV with Polars using detected dialect
                df = pl.read_csv(
                    io.BytesIO(file),
                    separator=dialect.delimiter,
                    quote_char=quote_char,
                    has_header=has_header,
                    infer_schema_length=10000,   # Good balance of speed/accuracy
                    ignore_errors=True,          # Skip malformed rows
                    truncate_ragged_lines=True,  # Handle uneven row lengths
                )
                if df.height < 5 or df.width < 2:
                    raise ValueError
                meta = {'content_type': 'text/csv'}

    # Unnest
    df = flatten_df(df)

    # Add row index column
    df = df.with_columns(pl.int_range(1, df.height + 1).alias("RowID")).select(
        ["RowID"] + df.columns
    )

    # Sanitize
    df = df.with_columns([
        pl.col(c)
          .cast(pl.Utf8, strict=False)
          .fill_null("")
          .str.replace_all(r"\s+", " ")
          .str.replace_all(r"\|", "¦")
          .str.strip_chars()
          .alias(c)
        for c in df.columns
    ])

    meta |= {"num_rows": df.height, "num_cols": len(df.columns), "columns": df.columns}
    markdown = df_to_markdown(df)

    return markdown, meta


def flatten_df(df: pl.DataFrame) -> pl.DataFrame:
    while any(isinstance(dtype, (pl.Struct, pl.List)) for dtype in df.dtypes):
        for col in df.columns:
            if isinstance(df[col].dtype, pl.Struct):
                unnested = df[col].struct.unnest()
                unnested = unnested.rename({c: f"{col}.{c}" for c in unnested.columns})
                df = df.drop(col).hstack(unnested)
            elif isinstance(df[col].dtype, pl.List):
                prim_type = df[col].dtype.inner.is_numeric() or df[col].dtype.inner == pl.Boolean
                max_len_4 = (df[col].list.len().max() or 0) > 4
                if prim_type and max_len_4:
                    df = df.drop(col)
                else:
                    df = df.with_columns(
                        pl.col(col)
                        .map_elements(stringify_list_value, return_dtype=pl.Utf8)
                        .alias(col)
                    )
    return df


def stringify_list_value(values) -> str:
    if values is None:
        return ""
    return ", ".join("" if value is None else str(value) for value in values)
    

def df_to_markdown(df: pl.DataFrame) -> str:
    lines = [" | ".join(df.columns), " | ".join("-" for _ in df.columns)]
    for row in df.iter_rows():
        cells = [format_tabular_cell(value) for value in row]
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def format_tabular_cell(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\r", " ").replace("\n", " ")