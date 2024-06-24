# Databricks notebook source
# MAGIC %md ###### Dataframe created from data about passengers on the Titanic

# COMMAND ----------

# If not using Databricks Notebook install Pandas with
# pip install pandas

# Load library
import pandas as pd

# Create URL
url = 'https://raw.githubusercontent.com/chrisalbon/sim_data/master/titanic.csv'

# Load data
dataframe = pd.read_csv(url)

# COMMAND ----------

# View the first few lines of data
print(dataframe.head())

# COMMAND ----------

# Show dimensions
dataframe.shape

# COMMAND ----------

# Show info
dataframe.info()

# COMMAND ----------

# Show statistics
dataframe.describe()

# COMMAND ----------

# MAGIC %md ###### Slicing DataFrames

# COMMAND ----------

# Select first row
dataframe.iloc[0]

# COMMAND ----------

# Select three rows
dataframe.iloc[1:4]

# COMMAND ----------

# MAGIC %md ##### Selecting Rows Based on Conditionals
# MAGIC

# COMMAND ----------

# Show top five rows where column 'sex' is 'male'
dataframe[dataframe['Sex'] == 'male'].head()

# COMMAND ----------

# MAGIC %md ###### Sorting Values
# MAGIC

# COMMAND ----------

# Sort the dataframe by age, show five rows
dataframe.sort_values(by=["Age"]).head()

# COMMAND ----------

# Filter rows, male aged 70 and over
dataframe[(dataframe['Sex'] == 'male') & (dataframe['Age'] >= 70)]

# COMMAND ----------

# MAGIC %md ##### Replacing Values

# COMMAND ----------

# Replace "female" and "male" with "Woman" and "Man"
dataframe['Sex'].replace(["female", "male"], ["Woman", "Man"]).head()

# COMMAND ----------

# Replace values, show five rows
dataframe.replace(1, "One").head()

# COMMAND ----------

# Replace values, show five rows
dataframe.replace(r"1st", "First", regex=True).head()

# COMMAND ----------

# MAGIC %md ##### Renaming Columns

# COMMAND ----------

# Rename column, show five rows
dataframe.rename(columns={'PClass': 'Passenger Class'}).head()

# COMMAND ----------

# Rename columns, show five rows
dataframe.rename(columns={'PClass': 'Passenger Class', 'Sex': 'Gender'}).head()

# COMMAND ----------

# MAGIC %md ###### Finding the Minimum, Maximum, Sum, Average, and Count

# COMMAND ----------

# Calculate statistics
print('Maximum:', dataframe['Age'].max())
print('Minimum:', dataframe['Age'].min())
print('Mean:', dataframe['Age'].mean())
print('Sum:', dataframe['Age'].sum())
print('Count:', dataframe['Age'].count())

# COMMAND ----------

# MAGIC %md ##### Finding Unique Values

# COMMAND ----------

# Select unique values
dataframe['Sex'].unique()

# COMMAND ----------

# Show counts
dataframe['Sex'].value_counts()

# COMMAND ----------

# MAGIC %md ###### Handling Missing Values

# COMMAND ----------

## Select missing values, show the first five rows
dataframe[dataframe['Age'].isnull()].head()

# COMMAND ----------

# MAGIC %md ##### Deleting a Column

# COMMAND ----------

# Delete column
dataframe.drop('Age', axis=1).head()

# COMMAND ----------

# Drop multiple columns once
dataframe.drop(['Age', 'Sex'], axis=1).head()

# COMMAND ----------

# Drop column by index
dataframe.drop(dataframe.columns[1], axis=1).head()

# COMMAND ----------

# MAGIC %md ##### Deleting a Row

# COMMAND ----------

# Delete rows, show first five rows of output
dataframe[dataframe['Sex'] != 'male'].head()

# COMMAND ----------

# MAGIC %md ###### Dropping Duplicate Rows
# MAGIC

# COMMAND ----------

# Drop duplicates, show first five rows of output
dataframe.drop_duplicates().head()

# COMMAND ----------

# MAGIC %md ###### Grouping Rows by Values

# COMMAND ----------

# Group rows by the values of the column 'Sex', calculate mean number of each group
dataframe.groupby('Sex').mean(numeric_only=True)

# COMMAND ----------

# Group rows, count rows
dataframe.groupby('Survived')['Name'].count()

# COMMAND ----------

# Group rows, calculate mean
dataframe.groupby(['Sex','Survived'])['Age'].mean()

# COMMAND ----------

# MAGIC %md ###### Aggregating Operations and Statistics
# MAGIC

# COMMAND ----------

# Get the minimum of every column
dataframe.agg("min")

# COMMAND ----------

# Mean Age, min and max SexCode
dataframe.agg({"Age":["mean"], "SexCode":["min", "max"]})

# COMMAND ----------

# Number of people who survived and didn't survive in each class
dataframe.groupby(
    ["PClass","Survived"]).agg({"Survived":["count"]}
  ).reset_index()

# COMMAND ----------

# MAGIC %md ##### Looping over a column
# MAGIC

# COMMAND ----------

# Print the first 5 names upper case
for name in dataframe['Name'][0:5]:
    print(name.upper())
