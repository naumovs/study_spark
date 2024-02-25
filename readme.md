# Learn Spark with Scala Project #

### Use spark-submit to run program: ###

```
$SPARK_HOME/spark-submit --class CrimeAnalysis \
    --master #your way to run Spark applications \
    --deploy-mode #your way to deploy Spark applications \
    learnSparkBostonCrimes-assembly-0.1.jar \
    <crime_csv_full_path> <offense_codes_csv_full_path> <output_parquet_full_path>
```
### Requirements: ###
spark-core 3.2.1, Scala 2.12.18

### Links: ###
used dataset https://www.kaggle.com/AnalyzeBoston/crimes-in-boston