# Parquet Viewer Implementation Plan

## Overview
A professional Streamlit web application for viewing and exploring Parquet files with filtering, sorting, pagination, and export capabilities.

## Project Structure
```
parquet_viewer/
├── app.py              # Main Streamlit application
├── requirements.txt    # Python dependencies
├── launch.bat          # Windows batch file for easy launching
└── sample_files/       # Example parquet files
    ├── NQ_12-24.parquet
    └── l2_data_20251231_181020_0083.parquet
```

## Features

### 1. File Upload
- Drag and drop parquet file upload widget
- Support for .parquet extension only
- Display uploaded filename
- Store file in session state for persistence

### 2. Schema View (Tab 1)
Display a table with:
- Column name
- Data type
- Nullable (Yes/No)

### 3. Data View (Tab 2)
Tabular display with:
- Pagination controls (page size: 10/25/50/100)
- Previous/Next navigation
- Page indicator (Page X of Y)
- Total rows display
- Filtered rows indicator (if applicable)

### 4. Metadata View (Tab 3)
Display:
- Format version
- Created by (software that wrote the file)
- Compression codec
- Number of row groups
- Total rows
- Serialized size

### 5. Filtering (Sidebar)
- Column selection dropdown
- Filter value text input
- Filter mode: Contains (for strings) or Equals
- Clear filter button
- Active filter indicator

### 6. Sorting (Sidebar)
- Column selection dropdown
- Sort direction: Ascending/Descending
- Clear sort button
- Active sort indicator

### 7. CSV Export (Sidebar)
- Download filtered view as CSV
- Download full dataset as CSV
- Filename includes original name + timestamp

## UI Design Guidelines

### Professional Styling
- NO emojis anywhere in the UI or code
- Clean, minimal interface
- Use Streamlit's default theme
- Clear section headers
- Consistent spacing

### Layout
```
+------------------+-------------------------------+
|   SIDEBAR        |         MAIN CONTENT          |
+------------------+-------------------------------+
| File Upload      |                               |
| [Uploader]       |  Tabs: [Schema][Data][Meta]   |
|                  |                               |
| ---              |                               |
| Filter Data      |  [Table Display]              |
| Column: [___]    |                               |
| Value:  [___]    |  Pagination: [10] [<] [>]     |
| [Clear]          |  Page 1 of 5 (50 rows)        |
|                  |                               |
| ---              |                               |
| Sort Data        |                               |
| Column: [___]    |                               |
| Dir: [Asc/Desc]  |                               |
| [Clear]          |                               |
|                  |                               |
| ---              |                               |
| Download         |                               |
| [Download CSV]   |                               |
+------------------+-------------------------------+
```

## Technical Implementation

### Dependencies
- streamlit: Web application framework
- pyarrow: Parquet file reading and metadata
- pandas: Data manipulation and display

### Session State Variables
```python
st.session_state.uploaded_file      # The uploaded file object
st.session_state.df                 # Pandas DataFrame
st.session_state.parquet_file       # PyArrow ParquetFile object
st.session_state.filter_column      # Selected filter column
st.session_state.filter_value       # Filter value input
st.session_state.sort_column        # Selected sort column
st.session_state.sort_ascending     # Sort direction (bool)
st.session_state.page_size          # Rows per page (int)
st.session_state.current_page       # Current page number (int)
```

### Key Functions

#### load_parquet(file)
- Reads uploaded file with pyarrow
- Converts to pandas DataFrame
- Stores in session state
- Returns success/failure

#### get_schema_dataframe(parquet_file)
- Extracts schema from PyArrow file
- Returns DataFrame with columns: Name, Type, Nullable

#### get_metadata_dict(parquet_file)
- Extracts metadata from PyArrow file
- Returns dictionary of metadata properties

#### apply_filters(df, column, value)
- Applies column filter to DataFrame
- Supports partial match for strings
- Returns filtered DataFrame

#### apply_sort(df, column, ascending)
- Sorts DataFrame by column
- Returns sorted DataFrame

#### paginate_dataframe(df, page, page_size)
- Slices DataFrame for current page
- Returns tuple: (page_df, total_pages)

### File Processing Flow
1. User uploads file -> store in session state
2. Read with PyArrow -> extract schema, metadata, data
3. Convert data to pandas DataFrame
4. Apply any active filters -> update display
5. Apply any active sort -> update display
6. Paginate -> display current page

### Error Handling
- Invalid file format: Show error message
- Empty file: Show warning
- Filter with no results: Show informational message
- Large files: Add warning about performance

## Implementation Steps

1. Create requirements.txt with dependencies
2. Create app.py with main application structure
3. Implement file upload functionality
4. Implement schema viewer tab
5. Implement metadata viewer tab
6. Implement data viewer with pagination
7. Implement filtering controls in sidebar
8. Implement sorting controls in sidebar
9. Implement CSV download functionality
10. Create launch.bat for easy startup
11. Test with provided sample files

## Code Style Guidelines
- Clear, descriptive variable names
- Function docstrings for all functions
- Comments explaining non-obvious logic
- NO emojis in code or UI
- Consistent formatting
- Type hints where appropriate
