import argparse
import pandas as pd


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
        help="Decimal character used in CSV files (default '.')",
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
            "'revenue_per_conversion'.",
        ),
    )
    return parser.parse_args()


def load_table(path: str, sep: str, decimal: str) -> pd.DataFrame:
    """Loads a CSV or Excel file into a DataFrame."""
    if path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, sep=sep, decimal=decimal)
    # Drop common index columns written by pandas.to_csv
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    # Ensure numeric columns are read as numeric when possible
    df = df.apply(pd.to_numeric, errors="coerce")
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
    media_df = load_table(args.media, args.sep, args.decimal)
    extra_df = load_table(args.extra, args.sep, args.decimal)

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
    if "population" not in merged.columns:
        merged["population"] = 1
    merged = rename_kpi_columns(
        merged,
        args.kpi_column,
        args.revenue_column,
        args.population_column,
        args.compute_per_conversion,
    )

    # Convert numeric-like columns to proper numeric dtypes before saving
    merged = merged.apply(pd.to_numeric, errors="coerce")

    merged.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
