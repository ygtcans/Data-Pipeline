import pandas as pd
import numpy as np
import logging
from typing import Optional, List

# Logger configuration
logger = logging.getLogger(__name__)
if not logger.hasHandlers():  # Check if the logger already has handlers
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        """
        Initializes the DataCleaner with the input DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
        """
        self.df = df.copy()  # Copy the DataFrame to avoid unintended changes
        self.numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
        self.string_cols = self.df.select_dtypes(include=['object']).columns
        logger.info("DataCleaner initialized with DataFrame containing %d rows and %d columns.", self.df.shape[0], self.df.shape[1])

    def fill_missing(self, strategy: str = "median", columns: Optional[List[str]] = None):
        """
        Fills missing values in specified columns based on the chosen strategy.

        Args:
            strategy (str): The strategy for filling missing values ("median" or "mode").
            columns (List[str]): List of columns to fill; if None, all numeric/string columns are used.
        """
        try:
            if strategy == "median":
                target_cols = columns or self.numeric_cols
                for col in target_cols:
                    median_value = self.df[col].median()
                    self.df[col].fillna(median_value, inplace=True)
                    logger.info(f"Filled missing values in '{col}' with median value {median_value}.")
            elif strategy == "mode":
                target_cols = columns or self.string_cols
                for col in target_cols:
                    mode_value = self.df[col].mode()[0]
                    self.df[col].fillna(mode_value, inplace=True)
                    logger.info(f"Filled missing values in '{col}' with mode value '{mode_value}'.")
            else:
                raise ValueError("Invalid strategy. Choose 'median' or 'mode'.")
        except Exception as e:
            logger.error(f"Error while filling missing values: {e}")

    def cap_outliers(self, lower_percentile: float = 0.01, upper_percentile: float = 0.99, columns: Optional[List[str]] = None):
        """
        Caps outliers in numerical columns within the specified percentiles.

        Args:
            lower_percentile (float): Lower bound percentile.
            upper_percentile (float): Upper bound percentile.
            columns (List[str]): List of columns to cap; if None, all numeric columns are used.
        """
        try:
            target_cols = columns or self.numeric_cols
            for col in target_cols:
                lower_bound = self.df[col].quantile(lower_percentile)
                upper_bound = self.df[col].quantile(upper_percentile)
                self.df[col] = self.df[col].clip(lower=lower_bound, upper=upper_bound)
                logger.info(f"Capped outliers in '{col}' to the range ({lower_bound}, {upper_bound}).")
        except Exception as e:
            logger.error(f"Error while capping outliers: {e}")

    def remove_duplicate_rows(self):
        """
        Removes duplicate rows from the DataFrame.
        """
        try:
            before_rows = self.df.shape[0]
            self.df.drop_duplicates(inplace=True)
            after_rows = self.df.shape[0]
            logger.info(f"Removed {before_rows - after_rows} duplicate rows.")
        except Exception as e:
            logger.error(f"Error while removing duplicate rows: {e}")

    def remove_highly_correlated_features(self, threshold: float = 0.9):
        """
        Removes features with a correlation higher than the specified threshold.

        Args:
            threshold (float): Correlation threshold above which features are dropped.
        """
        try:
            if not self.numeric_cols.any():
                logger.info("No numeric columns available for correlation analysis.")
                return
            corr_matrix = self.df[self.numeric_cols].corr()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            to_drop = [col for col in upper.columns if any(upper[col].abs() > threshold)]
            self.df.drop(columns=to_drop, inplace=True)
            logger.info(f"Removed highly correlated features: {to_drop}.")
        except Exception as e:
            logger.error(f"Error while removing highly correlated features: {e}")

    def clean_data(self, strategy: str = 'median'):
        """Initiates the data cleaning process with the chosen filling strategy."""
        try:
            logger.info("Starting data cleaning process")
            self.cap_outliers()
            self.fill_missing(strategy=strategy)
            self.remove_duplicate_rows()
            self.remove_highly_correlated_features()
            logger.info("Data cleaning process completed")
            return self.df
        except Exception as e:
            logger.error(f"Error during data cleaning process: {e}")