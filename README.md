# International Debt Analysis Project

## Overview
This project analyzes international debt data using PySpark to uncover key insights about global debt patterns, country-specific debt burdens, and debt indicator trends. The analysis provides comprehensive statistics on total debt, country distributions, debt types, and repayment patterns.

## Project Structure

### Files Included:
- **`data.py`** - Main Python script with complete PySpark analysis
- **`data.ipynb`** - Jupyter notebook with step-by-step analysis and troubleshooting
- **`international_debt_with_missing_values.csv`** - Dataset containing international debt records

## Key Features

### Analysis Performed:
1. **Total Debt Calculation** - Sum of all debt across all countries
2. **Country Analysis** - Distinct countries and their debt distribution
3. **Debt Indicators** - Analysis of different debt types and their meanings
4. **Top Debtors** - Countries with highest total debt
5. **Average Debt by Indicator** - Comparison across debt categories
6. **Principal Repayments** - Countries making highest repayments
7. **Common Indicators** - Most frequently occurring debt types
8. **Trend Analysis** - Additional insights and data quality assessment

## Prerequisites

### Required Software:
- **Java 8 or 11** (Required for PySpark)
- **Python 3.7+**
- **PySpark**
- **Jupyter Notebook** (for notebook version)

### Python Dependencies:
```bash
pip install pyspark pandas numpy findspark
```

## Installation & Setup

### 1. Install Java
**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

**macOS:**
```bash
brew install openjdk@11
```

**Windows:**
Download from [Adoptium](https://adoptium.net/) or [Oracle](https://www.oracle.com/java/technologies/downloads/)

### 2. Set JAVA_HOME
```python
import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'  # Linux
# os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home'  # macOS
# os.environ['JAVA_HOME'] = 'C:/Program Files/Java/jdk-11'  # Windows
```

### 3. Run the Analysis

**Option 1: Using Python Script**
```bash
python data.py
```

**Option 2: Using Jupyter Notebook**
```bash
jupyter notebook data.ipynb
```

## Dataset Information

The dataset contains international debt statistics with the following columns:
- `country_name` - Name of the country/region
- `country_code` - Country code (3-letter)
- `indicator_name` - Description of the debt indicator
- `indicator_code` - Code representing the debt type
- `debt` - Debt amount in current US dollars

### Data Quality Notes:
- Contains 2,357 total records
- Approximately 10.7% of records have missing debt values
- Some records have missing country names (9.9%)
- Some records have missing indicator names (10.6%)

## Key Findings

### Major Insights:
1. **Total Global Debt**: $2.82 trillion across all countries
2. **Country Coverage**: 125 distinct countries and regions
3. **Top Debtor**: China leads with $266.46 billion in total debt
4. **Highest Repayments**: China also leads in principal repayments at $168.61 billion
5. **Most Common Indicator**: "PPG, official creditors (INT, current US$)" appears 107 times

### Debt Distribution:
- **Official Creditors**: $318.26 billion
- **Private Creditors**: $233.70 billion
- **Bilateral Debt**: $202.18 billion
- **Multilateral Debt**: $133.55 billion

### Regional Analysis:
- **South Asia**: $243.69 billion
- **Least Developed Countries**: $152.25 billion
- **Africa**: $34.16 billion

## Technical Implementation

### Spark Configuration:
- Uses PySpark for distributed data processing
- Configured for local execution with optimal memory settings
- Includes error handling and fallback to pandas if Spark fails

### Analysis Functions:
- Data loading with schema inference
- Statistical calculations (sum, average, count, standard deviation)
- Grouping and aggregation operations
- Pattern matching for debt type categorization
- Data quality assessment

## Troubleshooting

### Common Issues:

1. **Java Not Found**
   - Install Java 8 or 11
   - Set JAVA_HOME environment variable
   - Verify installation with `java -version`

2. **Spark Session Creation Failed**
   - Check Java installation
   - Verify JAVA_HOME is set correctly
   - The notebook includes fallback to pandas analysis

3. **Memory Issues**
   - Spark configuration includes memory limits
   - Adjust `spark.executor.memory` and `spark.driver.memory` if needed

## Results Interpretation

The analysis reveals:
- Significant debt concentration in major economies
- Important role of official creditors in international lending
- Data quality challenges that should be considered in decision-making
- Regional patterns in debt accumulation and repayment

## Future Enhancements

Potential improvements:
- Time-series analysis for debt trends over years
- Debt-to-GDP ratio calculations
- Regional comparative analysis
- Predictive modeling for debt sustainability
- Interactive visualizations

## License

This project is for educational and analytical purposes. Please ensure compliance with data usage terms when working with international financial data.

## Support

For issues with setup or analysis, refer to the troubleshooting section in the Jupyter notebook or check the PySpark documentation for configuration guidance.
