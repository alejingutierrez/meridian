import argparse
import pandas as pd


def _ensure_regular_time_index(
    df: pd.DataFrame, date_column: str, geo_column: str | None = None
) -> pd.DataFrame:
    """Ensures that the time column is regularly spaced.

    Missing time periods are inserted with NA values so that Meridian's
    input validator does not fail on irregular intervals.
    """

    df[date_column] = pd.to_datetime(df[date_column])
    if geo_column is not None and geo_column in df.columns:
        geos = df[geo_column].unique()
    else:
        geos = [None]

    frames = []
    for geo in geos:
        geo_df = df if geo is None else df[df[geo_column] == geo]
        times = geo_df[date_column].sort_values()
        if len(times) < 2:
            frames.append(geo_df)
            continue
        diffs = times.diff().dropna().dt.days
        freq = (
            int(diffs.mode().iloc[0]) if not diffs.mode().empty else int(diffs.iloc[0])
        )
        full_range = pd.date_range(times.min(), times.max(), freq=f"{freq}D")
        if geo is None:
            reindexed = geo_df.set_index(date_column).reindex(full_range)
            reindexed = reindexed.reset_index().rename(columns={"index": date_column})
        else:
            idx = pd.MultiIndex.from_product(
                [[geo], full_range], names=[geo_column, date_column]
            )
            reindexed = (
                geo_df.set_index([geo_column, date_column]).reindex(idx).reset_index()
            )
        frames.append(reindexed)

    result = pd.concat(frames, ignore_index=True)
    result[date_column] = result[date_column].dt.strftime("%Y-%m-%d")
    return result


def _aggregate_weekly(
    df: pd.DataFrame, date_column: str, geo_column: str | None = None
) -> pd.DataFrame:
    """Aggregates daily data to weekly using sums or averages as required.

    The week starts on Monday. Some survey metrics should be averaged over the
    week while all other numeric columns are summed.
    """

    df[date_column] = pd.to_datetime(df[date_column])
    df[date_column] = df[date_column] - pd.to_timedelta(
        df[date_column].dt.weekday, unit="D"
    )

    group_cols = [date_column]
    if geo_column is not None and geo_column in df.columns:
        group_cols.insert(0, geo_column)

    numeric_cols = df.select_dtypes(include="number").columns

    avg_cols = {
        "nps",
        "ins",
        "ces",
        "gqv",
        "haceb_marca_proximas_comprar",
        "haceb_marca_top_of_heart",
        "haceb_recordacion_top_of_mind",
    }

    agg_dict: dict[str, str] = {}
    for col in numeric_cols:
        if col in avg_cols or col.startswith("descuento"):
            agg_dict[col] = "mean"
        else:
            agg_dict[col] = "sum"

    aggregated = df.groupby(group_cols, as_index=False).agg(agg_dict)

    aggregated[date_column] = aggregated[date_column].dt.strftime("%Y-%m-%d")
    return aggregated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge media and extra feature files for Meridian"
    )
    parser.add_argument(
        "--media", required=True, help="Path to CSV/Excel containing media data"
    )
    parser.add_argument(
        "--extra", required=True, help="Path to CSV/Excel with extra features"
    )
    parser.add_argument(
        "--output", required=True, help="Path where the merged CSV will be saved"
    )
    parser.add_argument(
        "--date-column",
        default="time",
        help="Name of the date column used for merging (default: 'time')",
    )
    parser.add_argument(
        "--kpi-column",
        default="conversions",
        help="Column containing KPI values (will be renamed to 'conversions')",
    )
    parser.add_argument(
        "--revenue-column",
        default="revenue_per_conversion",
        help=(
            "Column with revenue per KPI values (renamed to"
            " 'revenue_per_conversion')"
        ),
    )
    parser.add_argument(
        "--population-column",
        default="population",
        help="Column with population values (renamed to 'population')",
    )
    parser.add_argument(
        "--sep",
        default=",",
        help="Field separator used in CSV files (default ',')",
    )
    parser.add_argument(
        "--decimal",
        default=".",
        help=(
            "Decimal character used when reading and writing CSV files " "(default '.')"
        ),
    )
    parser.add_argument(
        "--date-format",
        default=None,
        help=(
            "Pandas datetime format string for parsing the date column. If not "
            "provided, the format will be inferred automatically. In all cases "
            "the dates are converted to 'YYYY-MM-DD'."
        ),
    )
    parser.add_argument(
        "--compute-per-conversion",
        action="store_true",
        help=(
            "Divide revenue column by KPI column before renaming it to "
            "'revenue_per_conversion'."
        ),
    )
    parser.add_argument(
        "--aggregate-weekly",
        action="store_true",
        help=(
            "Aggregate daily rows to weekly values using Monday as the first day. "
            "Survey metrics and columns starting with 'descuento' are averaged; "
            "all other numeric columns are summed."
        ),
    )
    parser.add_argument(
        "--thousands",
        default=None,
        help="Thousands separator used in CSV files (default: None)",
    )
    return parser.parse_args()


def load_table(
    path: str, sep: str, decimal: str, date_column: str, thousands: str | None
) -> pd.DataFrame:
    """Loads a CSV or Excel file into a DataFrame.

    The function tries to cast numeric-like columns to proper numeric types
    while leaving the date column untouched. This avoids accidental conversion
    of date strings to ``NaN`` values which would later cause type errors when
    loading the data with ``CsvDataLoader``.
    """

    if path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(
            path,
            sep=sep,
            decimal=decimal,
            thousands=thousands,
            encoding="utf-8-sig",
            skipinitialspace=True,
        )

    # Drop common index columns written by pandas.to_csv
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df.columns = df.columns.str.strip()

    # Strip whitespace and trailing percent signs from object columns
    cols_to_clean = df.columns.difference([date_column])
    object_cols = df[cols_to_clean].select_dtypes("object").columns
    if not object_cols.empty:
        df[object_cols] = df[object_cols].apply(lambda c: c.str.strip().str.rstrip("%"))

    # Cast all columns except the date column to numeric when possible
    cols_to_convert = df.columns.difference([date_column])
    df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors="coerce")

    return df


def rename_kpi_columns(
    df: pd.DataFrame,
    kpi_col: str,
    revenue_col: str,
    population_col: str,
    compute_per_conversion: bool,
) -> pd.DataFrame:
    """Renames KPI-related columns to Meridian defaults if present.

    Raises:
        ValueError: If any of the provided column names are not found and the
            DataFrame doesn't already contain the expected Meridian column.
    """

    rename_map: dict[str, str] = {}
    missing: list[str] = []

    if kpi_col in df.columns:
        rename_map[kpi_col] = "conversions"
    elif "conversions" not in df.columns:
        missing.append(kpi_col)

    if revenue_col in df.columns:
        if compute_per_conversion and kpi_col in df.columns:
            with pd.option_context("mode.chained_assignment", None):
                df[revenue_col] = df[revenue_col] / df[kpi_col]
        rename_map[revenue_col] = "revenue_per_conversion"
    elif "revenue_per_conversion" not in df.columns:
        missing.append(revenue_col)

    if population_col in df.columns:
        rename_map[population_col] = "population"
    elif "population" not in df.columns:
        missing.append(population_col)

    if missing:
        raise ValueError(
            "Column(s) %s not found in the input files. "
            "Use the --kpi-column/--revenue-column/--population-column "
            "arguments to specify the correct names." % ", ".join(missing)
        )

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def main() -> None:
    args = parse_args()
    media_df = load_table(
        args.media, args.sep, args.decimal, args.date_column, args.thousands
    )
    extra_df = load_table(
        args.extra, args.sep, args.decimal, args.date_column, args.thousands
    )

    merge_cols = [args.date_column]
    if "geo" in media_df.columns and "geo" in extra_df.columns:
        merge_cols.append("geo")

    # Remove duplicate rows that would cause Cartesian products during merging
    for df in (media_df, extra_df):
        if df.duplicated(subset=merge_cols).any():
            df.drop_duplicates(subset=merge_cols, inplace=True)

    for df in (media_df, extra_df):
        df[args.date_column] = pd.to_datetime(
            df[args.date_column], format=args.date_format
        ).dt.strftime("%Y-%m-%d")

    merged = pd.merge(media_df, extra_df, on=merge_cols, how="inner")

    if args.aggregate_weekly:
        merged = _aggregate_weekly(
            merged,
            date_column=args.date_column,
            geo_column="geo" if "geo" in merged.columns else None,
        )

    merged = _ensure_regular_time_index(
        merged,
        date_column=args.date_column,
        geo_column="geo" if "geo" in merged.columns else None,
    )
    if "population" not in merged.columns:
        merged["population"] = 1
    merged = rename_kpi_columns(
        merged,
        args.kpi_column,
        args.revenue_column,
        args.population_column,
        args.compute_per_conversion,
    )

    # Fill NaN values in media columns with a small value to avoid
    # downstream validation errors when loading the CSV with Meridian.
    media_like = (
        merged.columns.str.contains("impression", case=False)
        | merged.columns.str.contains("spend", case=False)
        | merged.columns.str.contains("investment", case=False)
    )
    media_cols = merged.columns[media_like]
    if not media_cols.empty:
        merged[media_cols] = merged[media_cols].fillna(0.001)

    # Convert numeric-like columns to proper numeric dtypes before saving.
    # Exclude the date column to preserve its "YYYY-MM-DD" format.
    cols_to_convert = merged.columns.difference([args.date_column])
    merged[cols_to_convert] = merged[cols_to_convert].apply(
        pd.to_numeric, errors="coerce"
    )

    # Replace any remaining NA values with a small positive number to
    # avoid validation errors when loading the data with Meridian. This is
    # applied to all columns other than the date column.
    merged[cols_to_convert] = merged[cols_to_convert].fillna(0.001)

    merged.to_csv(
        args.output,
        index=False,
        sep=args.sep,
        decimal=args.decimal,
    )


if __name__ == "__main__":
    main()
