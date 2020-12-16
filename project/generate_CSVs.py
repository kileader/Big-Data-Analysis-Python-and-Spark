# This script goes takes data from a mongo database and creates three csv files

import pymongo
import csv


def main():
    # Connect to mongo via pymongo
    mongo_connection = pymongo.MongoClient() 

    # Use the Europe database
    europe_db = mongo_connection['Europe'] 
    
    # Declare variables for each collection
    products = europe_db['Products']
    sales = europe_db['Sales']
    territories = europe_db['Territories']

    # Call methods to create the CSV file for each collection
    create_csv_file(products.find(), 'europe_product.csv')
    create_csv_file(sales.find(), 'europe_sales.csv')
    create_csv_file(territories.find(), 'europe_territory.csv')

    # Close mongo connection
    mongo_connection.close() 


def create_csv_file(collection, filename):

    # Grab the column names
    dictionary_0 = collection[0]
    fields = list(dictionary_0.keys())
    fields.remove('_id')

    # Add Country Code to fields
    if 'CountryName' in fields or 'SalesTerritoryCountry' in fields:
        fields.append('CountryCode')

    # List of lists for the data of the csv file
    rows = []

    # Iterate through each dictionary in the collection
    for old_dictionary in collection: 

        # Delete the mongo ID
        del old_dictionary['_id']

        # Create new dictionary with removed commas from values
        new_dictionary = old_dictionary
        for field in fields:
            old_value = old_dictionary.get(field)
            if isinstance(old_value, str) and ',' in old_value:
                new_value = old_value.replace(',','')
                new_dictionary[field] = new_value
            
        
        # Add in country codes column and values
        if 'CountryName' in new_dictionary:
            if new_dictionary['CountryName'] == 'France':
                new_dictionary['CountryCode'] = 'FR'
            elif new_dictionary['CountryName'] == 'Germany':
                new_dictionary['CountryCode'] = 'DE'
            elif new_dictionary['CountryName'] == 'United Kingdom':
                new_dictionary['CountryCode'] = 'GB'
        elif 'SalesTerritoryCountry' in new_dictionary:
            if new_dictionary['SalesTerritoryCountry'] == 'France':
                new_dictionary['CountryCode'] = 'FR'
            elif new_dictionary['SalesTerritoryCountry'] == 'Germany':
                new_dictionary['CountryCode'] = 'DE'
            elif new_dictionary['SalesTerritoryCountry'] == 'United Kingdom':
                new_dictionary['CountryCode'] = 'GB'
        
        row = new_dictionary.values()
        rows.append(row)
    
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows)


if __name__ == "__main__":
    main()