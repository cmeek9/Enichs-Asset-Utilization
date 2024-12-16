# SMU Utilization and Meter Reading Aggregation Script

## Project Overview

This script calculates maximum service meter readings (SMU) and utilization per asset by consolidating data from multiple SMU sources. Its goal is to provide a more comprehensive and up-to-date record of SMU readings to enable better business decisions.

### Purpose
- To aggregate SMU readings from diverse sources, overcoming the limitations of relying solely on DBS data.
- To provide accurate and actionable insights by calculating asset utilization using the most recent and valid SMU readings.

## Script Overview

### 1. Data Extraction

The script includes a stored procedure that is combining all data from various sources containing machine SMU readings into a staging table.

#### Details:
- The extaction function returns the following:
  - Equipment serial numbers
  - SMU (Service Meter Unit) readings
  - Dates of readings
  - Data sources
- Date columns are converted to a standardized format.
- Duplicates are removed to ensure clean and unique data in the resulting DataFrames.

---

### 2. Data Cleaning and Processing

The core data processing is handled by the `process_and_clean_data` function and is something of a orchestrator for all cleaning and normalizing of data, which preps it for analysis.

#### Steps:
1. **Filtering and Sorting:**
   - Removes SMU values outside the valid range (1 to 200,000).
   - Sorts the data for optimized group-by operations.

2. **Latest SMU Detection:**
   - Keeps the latest SMU entry for each serial number and date.
   - Tracks the first known SMU date for each serial number.

3. **Difference Calculations:**
   - Computes differences in SMU and days between consecutive readings.
   - Normalizes daily SMU increases (`Daily_SMU`).

4. **Validation:**
   - Filters out invalid rows, such as negative or excessively high SMU differences.

5. **Data Enrichment:**
   - Merges back the earliest known SMU date.
   - Computes a hash for each row to uniquely identify it.

---

### 3. Average Daily Usage Calculation

The `calculate_average_hours_per_day` function computes utilization metrics for various time windows.  This is coded much in the same design process and clean data is.

#### Features:
- Calculates average daily SMU usage over multiple time periods (e.g., 10, 30, 90, 180, 365 days).
- Computes yearly utilization based on total SMU and days in operation.
- Aggregates weighted average daily usage across all time windows using predefined weights.
- Incorporates the latest SMU readings, source information, and additional metrics.

---

### 4. SQL Table Write

The cleaned and processed data, along with calculated metrics, is written to a SQL table.

#### Functionality:
- Uses the `write_new_data_to_sql` function to connect to a SQL server via SQLAlchemy.
- Writes data in chunks to optimize memory usage and ensure reliability during data transfer.
- Replaces the existing SQL table with the new, updated data.

---


# Open Questions / Next Steps

## Does the Current Implementation Fully Address the Last Known SMU?

### Key Consideration: Last Known SMU
The script has encountered challenges in accurately determining the **last (most recent) SMU reading**. While the current implementation ensures that the latest readings are selected based on grouped and sorted data, there is room to improve this step.

- **Recommendation:** 
  Consider isolating the detection of the **last known SMU** as a separate process. Perform this detection independently before merging the results into the rest of the pipeline for utilization and aggregation calculations. This separation could enhance the accuracy and reliability of the most recent SMU values used downstream.


Based on the current process, the **last known SMU** is determined by keeping the latest `Smu_Date` and corresponding `SMU` value. However, it may not account for edge cases such as:

- Out-of-sequence data entries.
- Discrepancies between SMU sources (e.g., conflicts in dates across sources).
- If the weighted averages can be optimized we might use that going forward.

