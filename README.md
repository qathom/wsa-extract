# WSA

## Installation
Git clone the repository and run SBT in order to install the dependencies.

## Requirements

- This project needs to read JSON files located in `input/` to accomplish the data processing. The "normalized" tweets are then available in `output/`.
- In order to get the tweet sentiment, the file **FEEL-1.csv** must exist in `input/`.
- The file **stopwords.txt** should exist in `input/` in order to remove useless stop words when searching for the tweet sentiment.
- Spark SQL output are CSV files also contained in `output/` directory.
