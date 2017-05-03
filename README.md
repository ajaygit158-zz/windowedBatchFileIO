The DAG for this example is
fileReader --> csvParser --> windowedOperatorForAverage --> FileOutputOperator

It will read a directory containing CSV files of customer data (id, name, timestamp, age) , parse it using csv parser, windowed operator will calculate average of age value. This value is written to separate files by output operator.
At start and end of each file, startBatch and endBatch tuples will be output. The input operator will also emit watermark control tuples required by windowed operator.

