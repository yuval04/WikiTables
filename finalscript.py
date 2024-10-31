import os
import csv
import json
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Generate a unique log file name and folder path based on the timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
folder_path = f'WIKI_Tables_{timestamp}'
log_file_path = os.path.join(folder_path, f'WIKI_Tables_log_{timestamp}.txt')
csv_file_path = os.path.join(folder_path, f'WIKI_Tables_{timestamp}.csv')
json_file_path = os.path.join(folder_path, f'WIKI_Tables_{timestamp}.json')

# Create the folder
os.makedirs(folder_path, exist_ok=True)



# Loading random tables from the wiki tables file
def load_random_tables(json_file_path, tables_number):
    file_size = os.path.getsize(json_file_path)
    random_tables = []
    SKIP_SECTIONS = ["see also", "references", "external links", "sources"]


    # Open the file with UTF-8 encoding and ignore errors
    with open(json_file_path, 'r', encoding='utf-8', errors='ignore') as file:
        while tables_number > 0:
            # Select random byte offsets
            offsets = random.sample(range(file_size), tables_number)

            for offset in offsets:
                try:
                    file.seek(offset)
                    file.readline()
                    line = file.readline()
                    if line:
                        valid_table = True
                        table = json.loads(line)
                        loaded_table = extract_table_metadata(table)

                        if loaded_table:
                            if loaded_table['sectionTitle'] != None:
                                if loaded_table['sectionTitle'].lower() in SKIP_SECTIONS:
                                    valid_table = False
                            else:
                                loaded_table['sectionTitle'] = ''

                            if loaded_table['tableCaption'] != None:
                                if loaded_table['tableCaption'].lower() in SKIP_SECTIONS:
                                    valid_table = False
                            else:
                                valid_table = False

                        if valid_table:
                            random_tables.append(extract_table_metadata(table))
                            tables_number -= 1

                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
    return random_tables

# Extract relevant data from a table
def extract_table_metadata(table):
    return {
        "tableId": table.get("_id"),
        "pgId": table.get("pgId"),
        "pgTitle": table.get("pgTitle"),
        "sectionTitle": table.get("sectionTitle"),
        "tableCaption": table.get("tableCaption"),
        "tableData": normalize_table(table.get("tableData")),
        "tableHeaders": normalize_table(table.get("tableHeaders"))
    }

# Normalize table to clean up the table structure
def normalize_table(table):
    normalized = []
    for row in table:
        normalized_row = [cell['text'].strip() if isinstance(cell, dict) else str(cell).strip() for cell in row]
        normalized.append(normalized_row)
    return normalized



# Write log entries to the log file comparing tables
def write_log_entry(log_entry):
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(log_entry + '\n\n')

# Process table with concurrency
def process_tables_concurrently(tables_list, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_table, metadata): metadata for metadata in tables_list}

        for future in as_completed(futures):
            metadata = futures[future]
            try:
                future.result()
            except Exception as e:
                continue

# Web scraping table and compare to the one in the old file
def process_table(metadata, print_tables=False):
    log_entry = f"Page Title: {metadata['pgTitle']}\nSection: {metadata['sectionTitle']}\nCaption: {metadata['tableCaption']}\n"

    # Check if the page exists
    page_exists, url = check_page_exists(metadata['pgTitle'])
    if not page_exists:
        log_entry += "Page does not exist\n"
        write_log_entry(log_entry)
        return None

    # Get the page content
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')
    new_table, message = extract_wikipedia_table(soup, metadata.get("tableCaption"), metadata.get("sectionTitle"))

    log_entry += message
    if new_table:
        comparison_result = compare_tables(metadata, new_table)
        log_entry += comparison_result

        # Write the table to the CSV and JSON files
        write_table_to_files(metadata, new_table)

        if print_tables:
            print(f"Page Title: {metadata['pgTitle']}\nSection: {metadata['sectionTitle']}\nCaption: {metadata['tableCaption']}\n")
            print(f"Original Table: {metadata['tableHeaders']}\n{metadata['tableData']}\n")
            print(f"New Table: {new_table['tableHeaders']}\n{new_table['tableData']}\n")
            write_log_entry(log_entry)
            return True

    # Write the log entry to the file after processing
    write_log_entry(log_entry)

# Web scraping - extract table data from the Wikipedia page
def extract_wikipedia_table(soup, table_caption, section_title):
    # Step 1: Find the specified section (h2 heading)
    section = None
    for h2 in soup.find_all('h2'):
        if section_title.lower() in h2.get_text(strip=True).lower():
            section = h2
            break

    # If the section is not found, try to find the specified caption
    if not section:
        if section_title.lower() == table_caption.lower():
            for h3 in soup.find_all(['h3', 'h4']):
                if table_caption.lower() in h3.get_text(strip=True).lower():
                    section = h3
                    break

        if not section:
            return None, "Section not found"

    # Step 2: Locate the relevant caption within the section's scope
    tables_in_section = []
    current = section.find_next()
    is_caption = False

    while current and current.name != 'h2':  # Loop until the next h2 (next section) or end of page
        # Check for a h3 with the desired caption title
        if current.name == 'h3':
            is_caption = True
            if table_caption.lower() in current.get_text(strip=True).lower():
                # Find tables associated with this caption
                tables_in_section = []
                sibling = current.find_next()
                while sibling and sibling.name != 'h2':
                    if sibling.name == 'table' and 'wikitable' in sibling.get('class', []):
                        tables_in_section.append(sibling)
                    sibling = sibling.find_next()
                break

        # If no caption is specified or located, find tables within the section scope
        if not is_caption and current.name == 'table' and 'wikitable' in current.get('class', []):
            caption = current.find('caption')
            if caption:
                caption = caption.text.replace('\n', '')
            if (caption and table_caption in caption) or (
                    table_caption == section_title and (caption is None or caption == '')):
                tables_in_section.append(current)

        current = current.find_next()

    if not tables_in_section:
        return None, "No tables found in the section"

    # Step 3: Check if a single table is found
    if len(tables_in_section) == 1:
        return parse_html_table(tables_in_section[0]), "The table was found"
    else:
        return None, "Cannot find the specific table: multiple tables in the caption found"

# Parse an HTML table into a list of rows while handling rowspan and colspan
def parse_html_table(table):
    table_header = []
    table_data = []
    rowspans = {}
    num_cols = 0

    rows = table.find_all('tr')
    for row_idx, row in enumerate(rows):
        row_data = []
        col_idx = 0
        cell_idx = 0

        headers_cells = row.find_all(['th'])
        data_cells = row.find_all(['td'])

        while col_idx < len(headers_cells) and len(data_cells) == 0:
            colspan = int(headers_cells[col_idx].get('colspan', 1))
            for _ in range(colspan):
                for sup in headers_cells[col_idx].find_all('sup'):
                    sup.decompose()
                row_data.append(headers_cells[col_idx].get_text(strip=True).replace('\n', ''))
            col_idx += 1
            num_cols += colspan

        if 0 < len(headers_cells) and len(data_cells) == 0:
            if len(table_header) != 0:
                num_cols = num_cols - len(table_header[len(table_header) - 1])
            table_header.append(row_data)
        else:
            data_cells = row.find_all(['th', 'td'])
            while col_idx < num_cols and row_idx != 0:
                if col_idx in rowspans and rowspans[col_idx] and rowspans[col_idx].get('count') > 0:
                    # Fill row with previously recorded rowspan value
                    row_data.append(rowspans[col_idx].get('value'))
                    rowspans[col_idx] = {"count": rowspans[col_idx].get('count')-1, "value": rowspans[col_idx].get('value')}
                    col_idx += 1
                    continue

                cell = data_cells[cell_idx]

                # Handle rowspan and colspan attributes
                rowspan = int(cell.get('rowspan', 1))
                colspan = int(cell.get('colspan', 1))

                for sup in cell.find_all('sup'):
                    sup.decompose()

                cell_text = cell.get_text(strip=True).replace('\n', '')

                # If rowspan is greater than 1, save the cell content for future rows
                if rowspan > 1:
                    rowspans[col_idx] = {"count": rowspan - 1, "value": cell_text}

                # Add cell content to the current row (expand colspan horizontally)
                for _ in range(colspan):
                    if col_idx < num_cols:
                        row_data.append(cell_text)
                        col_idx += 1

                cell_idx += 1

            table_data.append(row_data)

    new_table = {
        "tableHeaders": table_header,
        "tableData": table_data,
    }
    return new_table

# Comparing tables
def compare_tables(old_table, new_table):
    result = ""
    is_changed = False

    # Compare the headers
    old_headers = old_table.get("tableHeaders", [])
    new_headers = new_table.get("tableHeaders", [])

    if old_headers != new_headers:
        is_changed = True
        result += f"\nDifference in the header:\nOld header: {old_headers}\nNew header: {new_headers}"

    # Compare row by row
    old_data = old_table.get("tableData", [])
    new_data = new_table.get("tableData", [])

    for row_idx, (old_row, new_row) in enumerate(zip(old_data, new_data)):
        if compare_rows(old_row, new_row) is False:
            is_changed = True
            result += f"\nDifference in row {row_idx + 1}:\nOld row: {old_row}\nNew row: {new_row}"

    if not is_changed:
        result += "\nThe table did not change."
    return result

# Function to check if the page exists
def check_page_exists(page_title):
    url = f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
    response = requests.get(url, timeout=5)
    return response.status_code == 200, url

# Comparing 2 rows
def compare_rows(old_row, new_row):
    # Normalize both rows by stripping whitespace and making lowercase
    normalized_old = ["".join(str(item).split()).lower() for item in old_row]
    normalized_new = ["".join(str(item).split()).lower() for item in new_row]

    return normalized_old == normalized_new

# Write tables to CSV and JSON files
def write_table_to_files(metadata, new_table):
    # Append to CSV
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Page Title:", metadata['pgTitle']])
        csv_writer.writerow(["Section:", metadata['sectionTitle']])
        csv_writer.writerow(["Caption:", metadata['tableCaption']])
        csv_writer.writerow(["Table:"])
        csv_writer.writerows(new_table['tableHeaders'])
        csv_writer.writerows(new_table['tableData'])
        csv_writer.writerow([])

    # Append to JSON
    table_entry = {
        "Page Title": metadata['pgTitle'],
        "Section": metadata['sectionTitle'],
        "Caption": metadata['tableCaption'],
        "Table": {
            "Table Headers": new_table['tableHeaders'],
            "Table Data": new_table['tableData']
        }
    }
    with open(json_file_path, 'a', encoding='utf-8') as json_file:
        json.dump(table_entry, json_file, ensure_ascii=False, indent=4)
        json_file.write(',\n')

# Initialize CSV
def initialize_csv(csv_file_path):
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)

# Initialize JSON
def initialize_json(json_file_path):
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json_file.write('[\n')



# Load random tables
file_path = 'C:/Users/yuval/Desktop/FinalProject/tables/tables.json'
tables_list = load_random_tables(file_path, tables_number=100)

# Initialize CSV and JSON
initialize_csv(csv_file_path)
initialize_json(json_file_path)

# Process tables concurrently to improve performance
process_tables_concurrently(tables_list, max_workers=15)

# Close JSON array properly
with open(json_file_path, 'a', encoding='utf-8') as json_file:
    json_file.write('\n]')





# ########################## check only 5 existing tables ##########################
# # Load random tables
# file_path = 'C:/Users/yuval/Desktop/FinalProject/tables/tables.json'
# tables_list = load_random_tables(file_path, tables_number=20)
#
# # Initialize CSV and JSON
# initialize_csv(csv_file_path)
# initialize_json(json_file_path)
#
# count = 0
# for table in tables_list:
#     try:
#         if process_table(table, True) == True:
#             count += 1
#         if count == 5:
#             break
#     except Exception as e:
#         continue
#
# # Close JSON array properly
# with open(json_file_path, 'a', encoding='utf-8') as json_file:
#     json_file.write('\n]')
