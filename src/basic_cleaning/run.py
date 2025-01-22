#!/usr/bin/env python
"""
A very basic data cleaning step.
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact.
"""

import argparse
import logging
import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def go(args):
    # Initialize a W&B run
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download the input artifact
    logger.info(f"Downloading input artifact: {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load the dataset
    logger.info(f"Reading data from {artifact_local_path}")
    df = pd.read_csv(artifact_local_path)

    # Drop outliers based on price
    logger.info(f"Filtering rows with price between {args.min_price} and {args.max_price}")
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Drop rows with missing values
    logger.info("Dropping rows with missing values")
    df = df.dropna()

    # Save the cleaned dataset
    output_file = "clean_sample.csv"
    logger.info(f"Saving cleaned data to {output_file}")
    df.to_csv(output_file, index=False)

    # Log the cleaned dataset to W&B
    logger.info(f"Logging artifact {args.output_artifact}")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(output_file)
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact containing the raw data (W&B artifact name)",
        required=True,
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for the output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the output artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to consider for filtering",
        required=True,
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price to consider for filtering",
        required=True,
    )

    args = parser.parse_args()

    go(args)