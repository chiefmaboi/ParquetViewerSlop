# Parquet Viewer

A professional Streamlit web application for viewing and exploring Apache Parquet files. Optimized for large files with millions of rows using lazy loading and efficient pagination.

## Features

- **Upload Parquet Files**: Drag and drop support for .parquet files
- **Data View**: Browse data with efficient pagination (10/25/50/100/500/1000 rows per page)
- **Schema View**: Display column names, data types, and nullability
- **Metadata View**: Show file format version, row count, compression info, and more
- **Column Filtering**: Filter data by column values with partial text matching
- **Column Pruning**: Select specific columns to display for better performance
- **Adjustable Settings**: Configure performance thresholds via the sidebar

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Quick Start (Windows)
Double-click `launch.bat` to start the application.

### Manual Start
```bash
streamlit run app.py
```

The application will open in your default web browser at `http://localhost:8501`.

### How to Use

1. **Upload a Parquet File**: Use the file uploader in the sidebar
2. **View Data**: The Data tab shows your data with pagination controls
3. **Filter Data**: Select a column and enter a filter value in the sidebar
4. **Select Columns**: Choose specific columns to display for better performance
5. **View Schema**: Switch to the Schema tab to see column information
6. **View Metadata**: Switch to the Metadata tab to see file details

## Performance Optimization

The application is optimized for large Parquet files:

- **Lazy Loading**: Only reads the rows needed for the current page
- **Row Group Based Reading**: For large files, reads only relevant row groups
- **Column Pruning**: Select specific columns to reduce memory usage
- **Configurable Thresholds**: Adjust when to use full load vs lazy loading

### Performance Settings

Access performance settings in the sidebar under "Settings":

- **Max rows for full load** (default: 100,000): Files with fewer rows are loaded entirely into memory for faster filtering
- Files above this threshold use row-group based lazy loading

## File Structure

```
parquet_viewer/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── launch.bat            # Windows launcher script
├── .streamlit/
│   └── config.toml       # Streamlit configuration (upload size limits)
└── README.md             # This file
```

## Configuration

### Streamlit Configuration

The `.streamlit/config.toml` file contains:
- `maxUploadSize`: Maximum file upload size (default: 2GB)

### Environment Variables

You can set these environment variables before running:
- `STREAMLIT_SERVER_PORT`: Change the default port (8501)
- `STREAMLIT_SERVER_ADDRESS`: Change the bind address

## Requirements

- streamlit>=1.28.0
- pyarrow>=14.0.0
- pandas>=2.0.0

## Browser Compatibility

Works with all modern browsers:
- Chrome/Edge (recommended)
- Firefox
- Safari

## Limitations

- Filtering large files loads data into memory, which may be slow for very large datasets
- Sorting is done on the currently displayed page only for large files
- Maximum upload size is 2GB by default (configurable in `.streamlit/config.toml`)

## Troubleshooting

### File Upload Too Large
If you encounter "File too large" errors:
1. Edit `.streamlit/config.toml`
2. Increase `maxUploadSize` value (in MB)
3. Restart the application

### Slow Performance with Large Files
- Use column pruning to select only needed columns
- Increase "Max rows for full load" setting if you have sufficient RAM
- Use smaller page sizes (10-25 rows) for faster loading

### Memory Issues
- Reduce "Max rows for full load" setting
- Select fewer columns in "Display Columns"
- Use pagination instead of filtering for very large files

## License

This project is provided as-is for educational and professional use.

## Support

For issues or questions, please check the troubleshooting section or review the Streamlit documentation at https://docs.streamlit.io
