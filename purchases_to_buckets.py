import pandas as pd
import numpy as np
import json
import time

# Start a timer to time the script.
start = time.time()

# Get the data.
print("Getting the data...")
purchase_names = ('order_id', 'isbn', 'publisher', 'school', 'price', 'duration', 'order_datetime')
bucket_names = ('publisher', 'price', 'duration')
purchases = pd.read_csv("purchase_data.csv", names = purchase_names)
buckets = pd.read_csv("purchase_buckets.csv", names = bucket_names)

print("Done.")
# To identify which purchases have been bucketed so that they don't get bucketed twice,
# we instantiate a 'bucketed' variable in the purchases DataFrame.
is_bucketed = pd.Series(False).repeat(len(purchases))
is_bucketed.index = range(len(is_bucketed))
purchases['is_bucketed'] = is_bucketed

# For the buckets dataframe we initialize a 'bucketed_purchases' column of empty lists.
buckets['bucketed_purchases'] = np.empty((len(buckets), 0)).tolist()

# Function to determine whether rows match for publisher.
# Inputs are rows from purchases and buckets.
# Output is boolean.
def is_publisher_match (purchase, bucket):
    if (bucket['publisher'] == '*'):
        return(True)
    return(purchase['publisher'].lower() == bucket['publisher'].lower())

# Function to determine whether rows match for price.
# Inputs are rows from purchases and buckets.
# Output is boolean.
def is_price_match (purchase, bucket):
    if (bucket['price'] == '*'):
        return(True)
    return(purchase['price'] == int(bucket['price']))

# Function to determine whether rows match for duration.
# Inputs are rows from purchases and buckets.
# Output is a boolean.
def is_duration_match (purchase, bucket):
    if (bucket['duration'] == '*'):
        return(True)
    return(purchase['duration'] == bucket['duration'])

# Method to copy purchase index to bucket's 'bucketed_purchases' list..
def copy_purchase_index_to_bucket_list(purchase, bucket):
    if not purchase.loc['is_bucketed']:
        bucket.loc['bucketed_purchases'].append(purchase.name)

# Method to set purchase as bucketed.
def set_purchase_bucketed(purchase):
    purchases.loc[purchase.name, 'is_bucketed'] = True

# Method to bucket_purchase purchase to bucket. (Does both of previous two.)
def bucket_purchase(purchase, bucket):
    copy_purchase_index_to_bucket_list(purchase, bucket)
    set_purchase_bucketed(purchase)

# Function to determine whether an order and bucket are a match.
def is_match (purchase, bucket):
    return (is_publisher_match(purchase, bucket) ==
            is_price_match(purchase, bucket) ==
            is_duration_match(purchase, bucket) ==
            True)

# Assign each bucket a priority, according to README criteria.
# Buckets with higher (numerically lower) priority will claim their
# purchases first.
# TODO: Do this algorithmically.
buckets['priority'] = 8
# All perfect criteria specified.
buckets.loc[(buckets['publisher'] != '*') &
            (buckets['price']     != '*') &
            (buckets['duration']  != '*'), 'priority'] = 0

# Only 'price' is wild.
buckets.loc[(buckets['publisher'] != '*') &
            (buckets['price']     == '*') &
            (buckets['duration']  != '*'), 'priority'] = 1

# Only 'duration' is wild.
buckets.loc[(buckets['publisher'] != '*') &
            (buckets['price']     != '*') &
            (buckets['duration']  == '*'), 'priority'] = 2

# Only 'publisher' is wild.
buckets.loc[(buckets['publisher'] == '*') &
            (buckets['price']     != '*') &
            (buckets['duration']  != '*'), 'priority'] = 3

# Only 'publisher' is specified.
buckets.loc[(buckets['publisher'] != '*') &
            (buckets['price']     == '*') &
            (buckets['duration']  == '*'), 'priority'] = 4

# Only 'duration' is specified.
buckets.loc[(buckets['publisher'] == '*') &
            (buckets['price']     == '*') &
            (buckets['duration']  != '*'), 'priority'] = 5

# Only 'price' is specified.
buckets.loc[(buckets['publisher'] == '*') &
            (buckets['price']     != '*') &
            (buckets['duration']  == '*'), 'priority'] = 6

# Nothing specified; all criteria wild.
buckets.loc[(buckets['publisher'] == '*') &
            (buckets['price']     == '*') &
            (buckets['duration']  == '*'), 'priority'] = 7

# Logic code to let each bucket of each priority level, in turn,
# claim matching purchases.  Only puchases not yet bucketed will be
# evaluated for potential matches.
def match_purchases_to_buckets():
    for priority in range(8):
        for ii in buckets.index[buckets['priority'] == priority].tolist():
            for jj in purchases.index[purchases['is_bucketed'] == False].tolist():
                if is_match(purchases.loc[jj], buckets.loc[ii]):
                    bucket_purchase(purchases.loc[jj], buckets.loc[ii])

print("Matching purchases to buckets...")
match_purchases_to_buckets()
print("Done.")

# Remove 'is_bucketed' variable from DataFrame
if (len(purchases[purchases['is_bucketed'] == False]) == 0):
    purchases = purchases.drop(['is_bucketed'], axis = 1)

# Now we that the purchases have all been bucketed,
# we format the results for conversion to JSON.

# Function to stringify a purchase.
def purchase_to_string(purchase):
    s = purchase.tolist()
    return(','.join(map(str,s)))

# Function to list stringifed purchases in each bucket.
def bucketed_purchases_to_list(bucket):
    output = list()
    for ii in bucket['bucketed_purchases']:
        output.append(purchase_to_string(purchases.loc[ii]))
    return(output)

# Function to stringify bucket.
def bucket_to_string(bucket):
    s = bucket[0:3].tolist()
    return(','.join(map(str,s)))

# Create DataFrame of buckets and purchases to be written to JSON (eventually).
buckets_Series = pd.Series(buckets.apply(bucket_to_string, axis = 1))
purchases_Series = pd.Series(buckets.apply(bucketed_purchases_to_list, axis = 1))
results_df = pd.concat([buckets_Series, purchases_Series], axis = 1)
results_df.columns = ['bucket', 'purchases']

# The pandas 'to_json' function formats output poorly, as of 05/20/2019.
# We convert the DataFrame to a dictionary briefly to use Python's
# 'json.dump' function which formats human-readable JSON with line
# returns and indentation.
print("Writing data to 'buckets.json'...")
results_dict = results_df.to_dict(orient = 'records')
with open('buckets.json', 'w') as outfile:
    json.dump(results_dict, outfile, indent = 4)
print("Done")

# End timer and print time.
end = time.time()
print("Execution time (in seconds):")
print(end - start)
print("See 'buckets.json' file for output.")
