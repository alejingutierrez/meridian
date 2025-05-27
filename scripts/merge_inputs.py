import argparse
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge media and extra feature files for Meridian"
    )
    parser.add_argument(
        "--media", required=True, help="Path to CSV containing media data"
    )
    parser.add_argument(
        "--extra", required=True, help="Path to CSV with extra features"
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
    return parser.parse_args()


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Drop common index columns written by pandas.to_csv
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    return df


def rename_kpi_columns(
    df: pd.DataFrame,
    kpi_col: str,
    revenue_col: str,
    population_col: str,
) -> pd.DataFrame:
    """Renames KPI-related columns to Meridian defaults if present."""
    rename_map = {}
    if kpi_col in df.columns:
        rename_map[kpi_col] = "conversions"
    if revenue_col in df.columns:
        rename_map[revenue_col] = "revenue_per_conversion"
    if population_col in df.columns:
        rename_map[population_col] = "population"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def main() -> None:
    args = parse_args()
    media_df = load_csv(args.media)
    extra_df = load_csv(args.extra)

    merge_cols = [args.date_column]
    if "geo" in media_df.columns and "geo" in extra_df.columns:
        merge_cols.append("geo")

    merged = pd.merge(media_df, extra_df, on=merge_cols, how="inner")
    merged = rename_kpi_columns(
        merged, args.kpi_column, args.revenue_column, args.population_column
    )

    merged.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
