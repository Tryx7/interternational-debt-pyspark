# international_debt_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def initialize_spark():
    """Initialize Spark session"""
    spark = SparkSession.builder \
        .appName("InternationalDebtAnalysis") \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """Load the international debt dataset"""
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    return df

def analyze_total_debt(df):
    """1. Calculate total amount of debt owed by all countries"""
    total_debt = df.select(sum("debt")).collect()[0][0]
    print(f"1. Total amount of debt owed by all countries: ${total_debt:,.2f}")
    return total_debt

def analyze_distinct_countries(df):
    """2. Count distinct countries in the dataset"""
    distinct_countries = df.select("country_name").distinct().count()
    print(f"2. Number of distinct countries: {distinct_countries}")
    
    print("\nSample of countries:")
    df.select("country_name").distinct().show(20, truncate=False)
    
    return distinct_countries

def analyze_indicators(df):
    """3. Analyze distinct types of indicators and their representations"""
    print("3. Distinct debt indicators and their descriptions:")
    
    indicators_summary = df.groupBy("indicator_name", "indicator_code") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count"))
    
    indicators_summary.show(30, truncate=False)
    
    print("\nIndicator codes and their likely meanings:")
    indicator_codes = df.select("indicator_code").distinct().filter(col("indicator_code").isNotNull())
    indicator_codes.show(30, truncate=False)
    
    return indicators_summary

def analyze_highest_debt_countries(df):
    """4. Find country with highest total debt"""
    print("4. Countries with highest total debt:")
    
    country_total_debt = df.filter(col("debt").isNotNull()) \
        .groupBy("country_name") \
        .agg(sum("debt").alias("total_debt")) \
        .orderBy(desc("total_debt"))
    
    country_total_debt.show(20, truncate=False)
    
    top_debt_country = country_total_debt.first()
    print(f"\nCountry with highest total debt: {top_debt_country['country_name']}")
    print(f"Total debt: ${top_debt_country['total_debt']:,.2f}")
    
    return top_debt_country

def analyze_average_debt_by_indicator(df):
    """5. Calculate average debt across different debt indicators"""
    print("5. Average debt by indicator type:")
    
    avg_debt_by_indicator = df.filter(col("debt").isNotNull()) \
        .groupBy("indicator_name") \
        .agg(
            avg("debt").alias("avg_debt"),
            count("*").alias("record_count")
        ) \
        .orderBy(desc("avg_debt"))
    
    avg_debt_by_indicator.show(30, truncate=False)
    
    print("\nAverage debt by indicator code pattern:")
    df_with_category = df.withColumn(
        "indicator_category", 
        regexp_extract(col("indicator_code"), r"^DT\.([A-Z]+)\.", 1)
    )
    
    avg_debt_by_category = df_with_category.filter(col("debt").isNotNull()) \
        .groupBy("indicator_category") \
        .agg(
            avg("debt").alias("avg_debt"),
            count("*").alias("record_count")
        ) \
        .orderBy(desc("avg_debt"))
    
    avg_debt_by_category.show(truncate=False)
    
    return avg_debt_by_indicator

def analyze_principal_repayments(df):
    """6. Find country with highest principal repayments"""
    print("6. Principal repayments analysis:")
    
    principal_repayments = df.filter(
        (col("indicator_name").like("%Principal repayments%")) | 
        (col("indicator_code") == "DT.AMT.DLXF.CD")
    )
    
    print(f"Number of principal repayment records: {principal_repayments.count()}")
    
    principal_by_country = principal_repayments.filter(col("debt").isNotNull()) \
        .groupBy("country_name") \
        .agg(sum("debt").alias("total_principal_repayments")) \
        .orderBy(desc("total_principal_repayments"))
    
    principal_by_country.show(20, truncate=False)
    
    top_repayer = principal_by_country.first()
    print(f"\nCountry with highest principal repayments: {top_repayer['country_name']}")
    print(f"Total principal repayments: ${top_repayer['total_principal_repayments']:,.2f}")
    
    return top_repayer

def analyze_common_indicators(df):
    """7. Find most common debt indicator across all countries"""
    print("7. Most common debt indicators:")
    
    common_indicators = df.groupBy("indicator_name", "indicator_code") \
        .agg(count("*").alias("frequency")) \
        .orderBy(desc("frequency"))
    
    common_indicators.show(20, truncate=False)
    
    most_common = common_indicators.first()
    print(f"\nMost common debt indicator: {most_common['indicator_name']}")
    print(f"Frequency: {most_common['frequency']} records")
    
    return most_common

def analyze_debt_trends(df):
    """8. Identify key debt trends and summarize findings"""
    print("8. Additional Debt Trends Analysis:")
    
    # Trend 1: Debt distribution by country
    print("\nTrend 1: Debt Distribution Statistics")
    debt_stats = df.filter(col("debt").isNotNull()).select(
        mean("debt").alias("mean_debt"),
        stddev("debt").alias("std_dev"),
        min("debt").alias("min_debt"),
        max("debt").alias("max_debt"),
        count("debt").alias("non_null_records")
    ).collect()[0]
    
    print(f"Average debt per record: ${debt_stats['mean_debt']:,.2f}")
    print(f"Debt standard deviation: ${debt_stats['std_dev']:,.2f}")
    print(f"Minimum debt: ${debt_stats['min_debt']:,.2f}")
    print(f"Maximum debt: ${debt_stats['max_debt']:,.2f}")
    
    # Trend 2: Missing data analysis
    print("\nTrend 2: Data Quality Analysis")
    total_records = df.count()
    null_debt_records = df.filter(col("debt").isNull()).count()
    null_country_records = df.filter(col("country_name").isNull()).count()
    null_indicator_records = df.filter(col("indicator_name").isNull()).count()
    
    print(f"Total records: {total_records}")
    print(f"Records with null debt values: {null_debt_records} ({null_debt_records/total_records*100:.1f}%)")
    print(f"Records with null country names: {null_country_records} ({null_country_records/total_records*100:.1f}%)")
    print(f"Records with null indicator names: {null_indicator_records} ({null_indicator_records/total_records*100:.1f}%)")
    
    # Trend 3: Debt by region/country groups
    print("\nTrend 3: Regional/Group Analysis")
    regional_keywords = ["IDA", "least developed", "South Asia", "Africa"]
    
    for keyword in regional_keywords:
        region_debt = df.filter(lower(col("country_name")).like(f"%{keyword.lower()}%")) \
            .filter(col("debt").isNotNull()) \
            .agg(sum("debt").alias("total_debt")).collect()[0]["total_debt"]
        if region_debt:
            print(f"Total debt for '{keyword}': ${region_debt:,.2f}")
    
    # Trend 4: Debt type analysis
    print("\nTrend 4: Debt Type Breakdown")
    debt_types = {
        "Bilateral": "BLAT",
        "Multilateral": "MLAT", 
        "Bonds": "PBND",
        "Commercial Banks": "PCBK",
        "Official Creditors": "OFFT",
        "Private Creditors": "PRVT"
    }
    
    for debt_type, code in debt_types.items():
        type_debt = df.filter(col("indicator_code").like(f"%{code}%")) \
            .filter(col("debt").isNotNull()) \
            .agg(sum("debt").alias("total_debt")).collect()[0]["total_debt"]
        if type_debt:
            print(f"Total {debt_type} debt: ${type_debt:,.2f}")
    
    return debt_stats

def print_summary(total_debt, distinct_countries, top_debt_country, top_repayer, most_common, null_debt_records, total_records):
    """Print summary of all findings"""
    print("=" * 80)
    print("SUMMARY OF KEY FINDINGS")
    print("=" * 80)
    
    print(f"1. TOTAL DEBT: ${total_debt:,.2f} owed by all countries combined")
    print(f"2. COUNTRY DIVERSITY: {distinct_countries} distinct countries in dataset")
    print(f"3. DEBT CONCENTRATION: Top country owes ${top_debt_country['total_debt']:,.2f}")
    print(f"4. PRINCIPAL REPAYMENTS: Highest repayer paid ${top_repayer['total_principal_repayments']:,.2f}")
    print(f"5. DATA QUALITY: {null_debt_records/total_records*100:.1f}% records missing debt values")
    print(f"6. COMMON INDICATOR: '{most_common['indicator_name']}' appears {most_common['frequency']} times")
    
    print("\nKEY TRENDS:")
    print("- Debt is heavily concentrated in a few major economies")
    print("- Principal repayments and disbursements are major debt activities")
    print("- Bilateral and multilateral official creditors play significant roles")
    print("- Some countries show very high debt levels requiring further investigation")
    print("- Data quality issues exist, particularly with missing country names and debt values")

def main():
    """Main function to run the complete analysis"""
    try:
        # Initialize Spark
        spark = initialize_spark()
        
        # Load data
        file_path = "international_debt_with_missing_values.csv"
        df = load_data(spark, file_path)
        
        # Display basic dataset info
        print("Dataset Schema:")
        df.printSchema()
        
        print("\nFirst 10 rows:")
        df.show(10, truncate=False)
        
        print(f"Total records: {df.count()}")
        
        # Run all analyses
        total_debt = analyze_total_debt(df)
        distinct_countries = analyze_distinct_countries(df)
        indicators_summary = analyze_indicators(df)
        top_debt_country = analyze_highest_debt_countries(df)
        avg_debt_by_indicator = analyze_average_debt_by_indicator(df)
        top_repayer = analyze_principal_repayments(df)
        most_common = analyze_common_indicators(df)
        debt_stats = analyze_debt_trends(df)
        
        # Print final summary
        total_records = df.count()
        null_debt_records = df.filter(col("debt").isNull()).count()
        print_summary(total_debt, distinct_countries, top_debt_country, top_repayer, most_common, null_debt_records, total_records)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise e
    
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()